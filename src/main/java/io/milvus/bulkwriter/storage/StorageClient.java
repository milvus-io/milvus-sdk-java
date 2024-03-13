package io.milvus.bulkwriter.storage;


import java.io.InputStream;

public interface StorageClient {
    Long getObjectEntity(String bucketName, String objectKey) throws Exception;
    boolean checkBucketExist(String bucketName) throws Exception;
    void putObjectStream(InputStream inputStream, long contentLength, String bucketName, String objectKey) throws Exception;
}
