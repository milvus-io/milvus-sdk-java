package io.milvus.bulkwriter.storage.client;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.milvus.bulkwriter.storage.StorageClient;
import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.credentials.StaticProvider;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.amazonaws.services.s3.internal.Constants.MB;

public class MinioStorageClient extends MinioClient implements StorageClient {
    private static final Logger logger = LoggerFactory.getLogger(MinioStorageClient.class);

    protected MinioStorageClient(MinioClient client, Multimap<String, String> extraHeader) {
        super(client);
    }

    public static MinioStorageClient getStorageClient(String endpoint,
                                                      String accessKey,
                                                      String secretKey,
                                                      String sessionToken,
                                                      String region,
                                                      OkHttpClient httpClient) {
        Multimap<String, String> extraHeader = HashMultimap.create();
        MinioClient.Builder minioClientBuilder = MinioClient.builder()
                .endpoint(endpoint)
                .credentialsProvider(new StaticProvider(accessKey, secretKey, sessionToken));

        if (StringUtils.isNotEmpty(region)) {
            minioClientBuilder.region(region);
        }

        if (httpClient != null) {
            minioClientBuilder.httpClient(httpClient);
        }

        return new MinioStorageClient(minioClientBuilder.build(), extraHeader);
    }

    public Long getObjectEntity(String bucketName, String objectKey) throws Exception {
        StatObjectArgs statObjectArgs = StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectKey)
                .build();
        StatObjectResponse statObject = statObject(statObjectArgs);
        return statObject.size();
    }

    public void putObjectStream(InputStream inputStream, long contentLength, String bucketName, String objectKey) throws Exception {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectKey)
                .stream(inputStream, contentLength, 5 * MB)
                .build();
        putObject(putObjectArgs);
    }

    public boolean checkBucketExist(String bucketName) throws Exception {
        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket(bucketName)
                .build();
        return bucketExists(bucketExistsArgs);
    }
}
