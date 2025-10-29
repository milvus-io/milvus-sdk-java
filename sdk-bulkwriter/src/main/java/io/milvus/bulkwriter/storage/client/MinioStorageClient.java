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
import io.minio.*;
import io.minio.credentials.StaticProvider;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import io.minio.http.Method;
import io.minio.messages.CompleteMultipartUpload;
import io.minio.messages.ErrorResponse;
import io.minio.messages.Part;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.amazonaws.services.s3.internal.Constants.MB;

public class MinioStorageClient extends MinioAsyncClient implements StorageClient {
    private static final Logger logger = LoggerFactory.getLogger(MinioStorageClient.class);
    private static final String UPLOAD_ID = "uploadId";


    protected MinioStorageClient(MinioAsyncClient client) {
        super(client);
    }

    public static MinioStorageClient getStorageClient(String cloudName,
                                                      String endpoint,
                                                      String accessKey,
                                                      String secretKey,
                                                      String sessionToken,
                                                      String region,
                                                      OkHttpClient httpClient) {
        MinioAsyncClient.Builder minioClientBuilder = MinioAsyncClient.builder()
                .endpoint(endpoint)
                .credentialsProvider(new StaticProvider(accessKey, secretKey, sessionToken));

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

        return new MinioStorageClient(minioClient);
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
        logger.info("uploading file, fileName:{}, size:{} bytes", file.getAbsolutePath(), file.length());
        FileInputStream fileInputStream = new FileInputStream(file);
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectKey)
                .stream(fileInputStream, file.length(), 5 * MB)
                .build();
        putObject(putObjectArgs).get();
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
