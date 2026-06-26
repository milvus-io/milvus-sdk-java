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

import static com.amazonaws.services.s3.internal.Constants.MB;

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

    public static MinioStorageClient getStorageClient(String cloudName,
                                                      String endpoint,
                                                      String accessKey,
                                                      String secretKey,
                                                      String sessionToken,
                                                      String region,
                                                      OkHttpClient httpClient) {
        boolean closeHttpClient = httpClient == null;
        MinioAsyncClient.Builder minioClientBuilder = MinioAsyncClient.builder()
                .endpoint(endpoint);

        if (CloudStorage.isGcpCloud(cloudName) && StringUtils.isNotEmpty(sessionToken)) {
            httpClient = buildAuthorizedClient(httpClient, sessionToken);
        } else {
            Provider credentialsProvider = new StaticProvider(accessKey, secretKey, sessionToken);
            minioClientBuilder.credentialsProvider(credentialsProvider);
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

    private static OkHttpClient buildAuthorizedClient(OkHttpClient httpClient, String sessionToken) {
        Interceptor authInterceptor = chain -> {
            Request original = chain.request();
            Request requestWithAuth = original.newBuilder()
                    .header("Authorization", "Bearer " + sessionToken)
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

    public Long getObjectEntity(String bucketName, String objectKey) throws Exception {
        StatObjectArgs statObjectArgs = StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectKey)
                .build();
        StatObjectResponse statObject = statObject(statObjectArgs).get();
        return statObject.size();
    }

    public void putObject(File file, String bucketName, String objectKey) throws Exception {
        putObject(file, bucketName, objectKey, null, 0L);
    }

    @Override
    public void putObject(File file, String bucketName, String objectKey,
                          UploadProgressListener progressListener) throws Exception {
        putObject(file, bucketName, objectKey, progressListener, 0L);
    }

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

    public boolean checkBucketExist(String bucketName) throws Exception {
        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket(bucketName)
                .build();
        return bucketExists(bucketExistsArgs).get();
    }

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
