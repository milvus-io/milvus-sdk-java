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

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.common.utils.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class AzureStorageClient implements StorageClient {
    private static final Logger logger = LoggerFactory.getLogger(AzureStorageClient.class);

    private final BlobServiceClient blobServiceClient;

    private AzureStorageClient(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    public static AzureStorageClient getStorageClient(String connStr,
                                                      String accountUrl,
                                                      TokenCredential credential) {
        BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder();
        if (credential != null) {
            blobServiceClientBuilder.credential(credential);
        }

        if (StringUtils.isNotEmpty(connStr)) {
            blobServiceClientBuilder.connectionString(connStr);
        } else if (StringUtils.isNotEmpty(accountUrl)) {
            blobServiceClientBuilder.endpoint(accountUrl);
        } else {
            ExceptionUtils.throwUnExpectedException("Illegal connection parameters");
        }
        BlobServiceClient blobServiceClient = blobServiceClientBuilder.buildClient();
        logger.info("Azure blob storage client successfully initialized");
        return new AzureStorageClient(blobServiceClient);
    }

    public Long getObjectEntity(String bucketName, String objectKey) {
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectKey);
        return blobClient.getProperties().getBlobSize();
    }

    public void putObject(File file, String bucketName, String objectKey) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectKey);
        blobClient.upload(fileInputStream, file.length());
    }

    public boolean checkBucketExist(String bucketName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
        return blobContainerClient.exists();
    }

}
