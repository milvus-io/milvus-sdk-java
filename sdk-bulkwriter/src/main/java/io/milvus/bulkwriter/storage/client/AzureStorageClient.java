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

/**
 * {@link StorageClient} implementation backed by the Azure Blob Storage SDK for uploading
 * bulk-import data files to Azure cloud storage.
 *
 * <p>The client connects to Azure Blob Storage using either a connection string, an account
 * endpoint, or a token credential, and maps cloud-storage buckets and object keys onto Azure blob
 * containers and blobs.
 */
public class AzureStorageClient implements StorageClient {
    private static final Logger logger = LoggerFactory.getLogger(AzureStorageClient.class);

    private final BlobServiceClient blobServiceClient;

    private AzureStorageClient(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    /**
     * Creates an {@link AzureStorageClient} from the given connection parameters.
     *
     * @param connStr the Azure storage connection string, or {@code null}
     * @param accountUrl the Azure blob service endpoint, or {@code null}
     * @param credential the token credential, or {@code null}
     * @return the configured {@link AzureStorageClient}
     */
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

    /**
     * Returns the size in bytes of the blob stored in the container.
     *
     * @param bucketName the Azure blob container name
     * @param objectKey the blob name
     * @return the blob size in bytes
     */
    public Long getObjectEntity(String bucketName, String objectKey) {
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectKey);
        return blobClient.getProperties().getBlobSize();
    }

    /**
     * Uploads a local data file as a blob to the container.
     *
     * @param file the local data file to upload
     * @param bucketName the Azure blob container name
     * @param objectKey the blob name
     * @throws FileNotFoundException if the local data file does not exist
     */
    public void putObject(File file, String bucketName, String objectKey) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectKey);
        blobClient.upload(fileInputStream, file.length());
    }

    /**
     * Checks whether the given blob container exists in the storage account.
     *
     * @param bucketName the Azure blob container name
     * @return {@code true} if the container exists, {@code false} otherwise
     */
    public boolean checkBucketExist(String bucketName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
        return blobContainerClient.exists();
    }

}
