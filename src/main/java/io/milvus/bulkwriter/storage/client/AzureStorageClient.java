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

import java.io.InputStream;

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

    public void putObjectStream(InputStream inputStream, long contentLength, String bucketName, String objectKey) {
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectKey);
        blobClient.upload(inputStream, contentLength);
    }


    public boolean checkBucketExist(String bucketName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
        return blobContainerClient.exists();
    }
}
