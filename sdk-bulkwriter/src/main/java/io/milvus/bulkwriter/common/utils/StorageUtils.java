package io.milvus.bulkwriter.common.utils;

import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.exception.ParamException;

public class StorageUtils {
    public static String getObjectUrl(String cloudName, String bucketName, String objectPath, String region) {
        CloudStorage cloudStorage = CloudStorage.getCloudStorage(cloudName);
        switch (cloudStorage) {
            case AWS:
                return String.format("https://s3.%s.amazonaws.com/%s/%s", region, bucketName, objectPath);
            case GCP:
                return String.format("https://storage.cloud.google.com/%s/%s", bucketName, objectPath);
            case TC:
                return String.format("https://%s.cos.%s.myqcloud.com/%s", bucketName, region, objectPath);
            case ALI:
                return String.format("https://%s.oss-%s.aliyuncs.com/%s", bucketName, region, objectPath);
            default:
                throw new ParamException("no support others remote storage address");
        }
    }
}
