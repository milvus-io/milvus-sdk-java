/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.bulkwriter.storage.client;

import com.google.common.collect.Multimap;
import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.model.CompleteMultipartUploadOutputModel;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.exception.ParamException;
import io.minio.BucketExistsArgs;
import io.minio.MinioAsyncClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.S3Base;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.Xml;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import io.minio.http.Method;
import io.minio.messages.CompleteMultipartUpload;
import io.minio.messages.ErrorResponse;
import io.minio.messages.Part;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.amazonaws.services.s3.internal.Constants.MB;

/**
 * {@link StorageClient} implementation backed by a {@link MinioAsyncClient} for uploading
 * bulk-import data files to S3-compatible cloud storage (MinIO, AWS S3, Alibaba Cloud OSS,
 * Tencent Cloud COS, GCP, and other S3-compatible services).
 *
 * <p>The client uploads data files as objects into a cloud-storage bucket. Large files are split
 * into multipart parts sized to stay within the S3 part count limits, and an optional progress
 * listener reports upload progress. This class also adjusts the multipart upload completion flow
 * for full MinIO/S3 compatibility.
 */
public class MinioStorageClient extends MinioAsyncClient implements StorageClient {
    private static final Logger logger = LoggerFactory.getLogger(MinioStorageClient.class);
    private static final String UPLOAD_ID = "uploadId";
    private static final long MIN_MULTIPART_PART_SIZE = 5L * MB;
    private static final long TARGET_MULTIPART_PART_COUNT = 1000L;
    private static final long MAX_MULTIPART_PART_COUNT = 10000L;
    private final boolean closeHttpClient;

    protected MinioStorageClient(MinioAsyncClient client, boolean closeHttpClient) {
        super(client);
        this.closeHttpClient = closeHttpClient;
    }

    /**
     * Creates a {@link MinioStorageClient} from a full {@link S3ConnectParam}. When the param
     * carries an external credentials provider (e.g. AWS web identity or GCP workload identity),
     * the provider is used so credentials refresh transparently while the writer is running;
     * otherwise the static accessKey/secretKey/sessionToken are used as before.
     *
     * <p>For GCP the provider's {@code fetch()} is invoked per HTTP request (the bearer token
     * is attached by an interceptor), so providers must cache credentials internally, as the
     * minio {@link Provider} contract requires.
     *
     * @param connectParam the connection parameters
     * @return the configured {@link MinioStorageClient}
     */
    public static MinioStorageClient getStorageClient(S3ConnectParam connectParam) {
        return getStorageClient(connectParam.getCloudName(), connectParam.getEndpoint(),
                connectParam.getAccessKey(), connectParam.getSecretKey(), connectParam.getSessionToken(),
                connectParam.getRegion(), connectParam.getHttpClient(), connectParam.getCredentialsProvider());
    }

    /**
     * Creates a {@link MinioStorageClient} for the given cloud storage provider.
     *
     * @param cloudName the cloud storage name, used to detect GCP and Tencent Cloud behavior
     * @param endpoint the S3-compatible endpoint URL of the cloud storage service
     * @param accessKey the access key (not used for GCP when a session token is provided)
     * @param secretKey the secret key
     * @param sessionToken the session token, or {@code null}; for GCP this token is sent as a
     *                     bearer Authorization header
     * @param region the storage region, or {@code null}
     * @param httpClient an optional {@link OkHttpClient} to reuse, or {@code null} to create and
     *                   own one
     * @return the configured {@link MinioStorageClient}
     */
    public static MinioStorageClient getStorageClient(String cloudName,
                                                      String endpoint,
                                                      String accessKey,
                                                      String secretKey,
                                                      String sessionToken,
                                                      String region,
                                                      OkHttpClient httpClient) {
        return getStorageClient(cloudName, endpoint, accessKey, secretKey, sessionToken, region,
                httpClient, null);
    }

    // Shared implementation. When credentialsProvider is set it is used so credentials refresh
    // transparently while the writer runs (for GCP the bearer token is read from
    // Credentials.sessionToken() per request, so the provider must cache internally per the
    // minio Provider contract); otherwise the static accessKey/secretKey/sessionToken apply.
    private static MinioStorageClient getStorageClient(String cloudName,
                                                      String endpoint,
                                                      String accessKey,
                                                      String secretKey,
                                                      String sessionToken,
                                                      String region,
                                                      OkHttpClient httpClient,
                                                      Provider credentialsProvider) {
        boolean closeHttpClient = httpClient == null;
        MinioAsyncClient.Builder minioClientBuilder = MinioAsyncClient.builder()
                .endpoint(endpoint);

        if (CloudStorage.isGcpCloud(cloudName)
                && (credentialsProvider != null || StringUtils.isNotEmpty(sessionToken))) {
            // the GCS XML API authenticates with a bearer header instead of signing; the
            // bearer token rides in Credentials.sessionToken per the established convention
            httpClient = buildAuthorizedClient(httpClient, gcpBearerTokenSource(credentialsProvider, sessionToken));
        } else {
            Provider provider = credentialsProvider != null
                    ? credentialsProvider
                    : new StaticProvider(accessKey, secretKey, sessionToken);
            minioClientBuilder.credentialsProvider(provider);
        }

        if (StringUtils.isNotEmpty(region)) {
            minioClientBuilder.region(region);
        }

        if (httpClient != null) {
            minioClientBuilder.httpClient(httpClient);
        }

        MinioAsyncClient minioClient = minioClientBuilder.build();
        if (CloudStorage.isTcCloud(cloudName)) {
            minioClient.enableVirtualStyleEndpoint();
        }

        return new MinioStorageClient(minioClient, closeHttpClient);
    }

    private static Supplier<String> gcpBearerTokenSource(Provider credentialsProvider, String sessionToken) {
        if (credentialsProvider != null) {
            return () -> {
                String token = credentialsProvider.fetch().sessionToken();
                if (StringUtils.isEmpty(token)) {
                    throw new IllegalStateException("credentials provider returned no session token;"
                            + " for GCP the bearer token must be carried in Credentials.sessionToken()");
                }
                return token;
            };
        }
        return () -> sessionToken;
    }

    private static OkHttpClient buildAuthorizedClient(OkHttpClient httpClient, Supplier<String> tokenSupplier) {
        Interceptor authInterceptor = chain -> {
            Request original = chain.request();
            Request requestWithAuth = original.newBuilder()
                    .header("Authorization", "Bearer " + tokenSupplier.get())
                    .build();
            return chain.proceed(requestWithAuth);
        };

        if (httpClient != null) {
            return httpClient.newBuilder()
                    .addInterceptor(authInterceptor)
                    .build();
        } else {
            return new OkHttpClient.Builder()
                    .addInterceptor(authInterceptor)
                    .build();
        }
    }

    /**
     * Returns the size in bytes of the object stored in the bucket.
     *
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @return the object size in bytes
     * @throws Exception if the object cannot be statted
     */
    public Long getObjectEntity(String bucketName, String objectKey) throws Exception {
        StatObjectArgs statObjectArgs = StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectKey)
                .build();
        StatObjectResponse statObject = statObject(statObjectArgs).get();
        return statObject.size();
    }

    /**
     * Uploads a local data file as an object to the bucket without progress reporting.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @throws Exception if the upload fails
     */
    public void putObject(File file, String bucketName, String objectKey) throws Exception {
        putObject(file, bucketName, objectKey, null, 0L);
    }

    /**
     * Uploads a local data file as an object to the bucket, reporting progress.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @param progressListener the listener notified of upload progress, or {@code null}
     * @throws Exception if the upload fails
     */
    @Override
    public void putObject(File file, String bucketName, String objectKey,
                          UploadProgressListener progressListener) throws Exception {
        putObject(file, bucketName, objectKey, progressListener, 0L);
    }

    /**
     * Uploads a local data file as an object to the bucket with progress reporting and an
     * optional explicit multipart part size.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @param progressListener the listener notified of upload progress, or {@code null}
     * @param partSizeBytes the multipart part size in bytes, or {@code 0} to auto-calculate
     * @throws Exception if the upload fails
     */
    @Override
    public void putObject(File file, String bucketName, String objectKey,
                          UploadProgressListener progressListener, long partSizeBytes) throws Exception {
        logger.info("uploading file, fileName:{}, size:{} bytes", file.getAbsolutePath(), file.length());
        long uploadPartSize = calculateUploadPartSize(file.length(), partSizeBytes);
        try (InputStream fileInputStream = new ProgressInputStream(new FileInputStream(file), progressListener)) {
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectKey)
                    .stream(fileInputStream, file.length(), uploadPartSize)
                    .build();
            putObject(putObjectArgs).get();
        }
    }

    /**
     * Releases the resources held by this client. When this client owns its HTTP client, the
     * connection pool is evicted and the executor service is shut down.
     */
    @Override
    public void close() {
        if (!closeHttpClient || httpClient == null) {
            return;
        }
        ExecutorService executorService = httpClient.dispatcher().executorService();
        executorService.shutdown();
        httpClient.connectionPool().evictAll();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while shutting down MinIO HTTP client executor", e);
        }
        if (httpClient.cache() != null) {
            try {
                httpClient.cache().close();
            } catch (IOException e) {
                logger.warn("Failed to close MinIO HTTP client cache", e);
            }
        }
    }

    static long calculateUploadPartSize(long fileSize, long requestedPartSizeBytes) {
        if (requestedPartSizeBytes > 0) {
            if (requestedPartSizeBytes < MIN_MULTIPART_PART_SIZE) {
                throw new ParamException("partSizeBytes must be at least " + MIN_MULTIPART_PART_SIZE + " bytes");
            }
            return requestedPartSizeBytes;
        }
        if (fileSize <= 0) {
            return MIN_MULTIPART_PART_SIZE;
        }
        long targetPartSize = divideCeil(fileSize, TARGET_MULTIPART_PART_COUNT);
        long maxPartCountSize = divideCeil(fileSize, MAX_MULTIPART_PART_COUNT);
        long partSize = Math.max(MIN_MULTIPART_PART_SIZE, Math.max(targetPartSize, maxPartCountSize));
        return divideCeil(partSize, MB) * MB;
    }

    private static long divideCeil(long value, long divisor) {
        return (value + divisor - 1) / divisor;
    }

    /**
     * Checks whether the given bucket exists in the cloud storage.
     *
     * @param bucketName the cloud-storage bucket name
     * @return {@code true} if the bucket exists, {@code false} otherwise
     * @throws Exception if the bucket existence cannot be determined
     */
    public boolean checkBucketExist(String bucketName) throws Exception {
        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket(bucketName)
                .build();
        return bucketExists(bucketExistsArgs).get();
    }

    /**
     * Completes a multipart upload for S3-compatible cloud storage.
     *
     * <p>Because MinIO follows the S3 protocol, the completion flow is adjusted to handle the
     * {@code uploadId} query parameter and to parse the returned XML into a
     * {@link CompleteMultipartUploadOutputModel}.
     *
     * @param bucketName the bucket name
     * @param region the storage region
     * @param objectName the object key
     * @param uploadId the multipart upload identifier
     * @param parts the uploaded parts to assemble
     * @param extraHeaders extra HTTP headers to send
     * @param extraQueryParams extra query parameters to send
     * @return a future resolving to the object write response
     * @throws InsufficientDataException if request data is incomplete
     * @throws InternalException if an internal MinIO error occurs
     * @throws InvalidKeyException if the signing key is invalid
     * @throws IOException if an I/O error occurs
     * @throws NoSuchAlgorithmException if the signing algorithm is unavailable
     * @throws XmlParserException if the response XML cannot be parsed
     */
    @Override
    // Considering MinIO's compatibility with S3, some adjustments have been made here.
    protected CompletableFuture<ObjectWriteResponse> completeMultipartUploadAsync(String bucketName, String region, String objectName, String uploadId, Part[] parts, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        Multimap<String, String> queryParams = newMultimap(extraQueryParams);
        queryParams.put(UPLOAD_ID, uploadId);
        return getRegionAsync(bucketName, region)
                .thenCompose(
                        location -> {
                            try {
                                return executeAsync(
                                        Method.POST,
                                        bucketName,
                                        objectName,
                                        location,
                                        httpHeaders(extraHeaders),
                                        queryParams,
                                        new CompleteMultipartUpload(parts),
                                        0);
                            } catch (InsufficientDataException
                                     | InternalException
                                     | InvalidKeyException
                                     | IOException
                                     | NoSuchAlgorithmException
                                     | XmlParserException e) {
                                throw new CompletionException(e);
                            }
                        })
                .thenApply(
                        response -> {
                            try {
                                String bodyContent = response.body().string();
                                bodyContent = bodyContent.trim();
                                if (!bodyContent.isEmpty()) {
                                    try {
                                        if (Xml.validate(ErrorResponse.class, bodyContent)) {
                                            ErrorResponse errorResponse = Xml.unmarshal(ErrorResponse.class, bodyContent);
                                            throw new CompletionException(
                                                    new ErrorResponseException(errorResponse, response, null));
                                        }
                                    } catch (XmlParserException e) {
                                        // As it is not <Error> message, fallback to parse CompleteMultipartUploadOutput
                                        // XML.
                                    }

                                    try {
                                        CompleteMultipartUploadOutputModel result =
                                                Xml.unmarshal(CompleteMultipartUploadOutputModel.class, bodyContent);
                                        return new ObjectWriteResponse(
                                                response.headers(),
                                                result.bucket(),
                                                result.location(),
                                                result.object(),
                                                result.etag(),
                                                response.header("x-amz-version-id"));
                                    } catch (XmlParserException e) {
                                        // As this CompleteMultipartUpload REST call succeeded, just log it.
                                        java.util.logging.Logger.getLogger(S3Base.class.getName())
                                                .warning(
                                                        "S3 service returned unknown XML for CompleteMultipartUpload REST API. "
                                                                + bodyContent);
                                    }
                                }

                                return new ObjectWriteResponse(
                                        response.headers(),
                                        bucketName,
                                        region,
                                        objectName,
                                        null,
                                        response.header("x-amz-version-id"));
                            } catch (IOException e) {
                                throw new CompletionException(e);
                            } finally {
                                response.close();
                            }
                        });
    }
}

class ProgressInputStream extends FilterInputStream {
    private final StorageClient.UploadProgressListener progressListener;

    ProgressInputStream(InputStream inputStream, StorageClient.UploadProgressListener progressListener) {
        super(inputStream);
        this.progressListener = progressListener;
    }

    @Override
    public int read() throws IOException {
        int value = super.read();
        if (value != -1) {
            notifyProgress(1);
        }
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        int count = super.read(bytes, offset, length);
        if (count > 0) {
            notifyProgress(count);
        }
        return count;
    }

    private void notifyProgress(long bytes) {
        if (progressListener != null) {
            progressListener.onProgress(bytes);
        }
    }
}
