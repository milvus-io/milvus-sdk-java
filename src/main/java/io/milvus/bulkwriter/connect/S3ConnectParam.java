package io.milvus.bulkwriter.connect;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.NotNull;

/**
 * Parameters for <code>RemoteBulkWriter</code> interface.
 */
@Getter
@ToString
public class S3ConnectParam extends StorageConnectParam {
    private final String bucketName;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String region;
    private final OkHttpClient httpClient;
    private final String cloudName;

    private S3ConnectParam(@NonNull Builder builder) {
        this.bucketName = builder.bucketName;
        this.endpoint = builder.endpoint;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.sessionToken = builder.sessionToken;
        this.region = builder.region;
        this.httpClient = builder.httpClient;
        this.cloudName = builder.cloudName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link S3ConnectParam} class.
     */
    public static final class Builder {
        private String bucketName;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String sessionToken;
        private String region;
        private OkHttpClient httpClient;
        private String cloudName;

        private Builder() {
        }

        /**
         * Sets the cloudName.
         *
         * @param cloudName cloud name
         * @return <code>Builder</code>
         */
        public Builder withCloudName(@NotNull String cloudName) {
            this.cloudName = cloudName;
            return this;
        }

        /**
         * Sets the bucketName info.
         *
         * @param bucketName bucket info
         * @return <code>Builder</code>
         */
        public Builder withBucketName(@NonNull String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        /**
         * Sets the endpoint.
         *
         * @param endpoint endpoint info
         * @return <code>Builder</code>
         */
        public Builder withEndpoint(@NonNull String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder withAccessKey(@NonNull String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder withSecretKey(@NonNull String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder withSessionToken(@NonNull String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        public Builder withRegion(@NonNull String region) {
            this.region = region;
            return this;
        }

        public Builder withHttpClient(@NonNull OkHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link S3ConnectParam} instance.
         *
         * @return {@link S3ConnectParam}
         */
        public S3ConnectParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(endpoint, "endpoint");
            ParamUtils.CheckNullEmptyString(bucketName, "bucketName");

            return new S3ConnectParam(this);
        }
    }
}
