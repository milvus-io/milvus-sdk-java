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

package io.milvus.bulkwriter.connect;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.NotNull;

/**
 * Parameters for <code>RemoteBulkWriter</code> interface.
 */
public class S3ConnectParam extends StorageConnectParam {
    private final String bucketName;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String region;
    private final OkHttpClient httpClient;
    private final String cloudName;

    private S3ConnectParam(@NotNull Builder builder) {
        this.bucketName = builder.bucketName;
        this.endpoint = builder.endpoint;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.sessionToken = builder.sessionToken;
        this.region = builder.region;
        this.httpClient = builder.httpClient;
        this.cloudName = builder.cloudName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public String getRegion() {
        return region;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public String getCloudName() {
        return cloudName;
    }

    @Override
    public String toString() {
        return "S3ConnectParam{" +
                "bucketName='" + bucketName + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", sessionToken='" + sessionToken + '\'' +
                ", region='" + region + '\'' +
                ", cloudName='" + cloudName + '\'' +
                '}';
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
        public Builder withBucketName(@NotNull String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        /**
         * Sets the endpoint.
         *
         * @param endpoint endpoint info
         * @return <code>Builder</code>
         */
        public Builder withEndpoint(@NotNull String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder withAccessKey(@NotNull String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder withSecretKey(@NotNull String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder withSessionToken(@NotNull String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        public Builder withRegion(@NotNull String region) {
            this.region = region;
            return this;
        }

        public Builder withHttpClient(@NotNull OkHttpClient httpClient) {
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
