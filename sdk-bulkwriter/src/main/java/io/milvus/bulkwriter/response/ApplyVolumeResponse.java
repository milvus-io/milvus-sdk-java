package io.milvus.bulkwriter.response;

import java.io.Serializable;


/**
 * A response representing the result of applying for a cloud storage volume.
 *
 * <p>It contains the volume access information, temporary credentials, and the upload
 * constraints enforced by the volume service, and is used by the BulkWriter to upload
 * files before bulk importing them.</p>
 */
public class ApplyVolumeResponse implements Serializable {
    private String endpoint;
    private String cloud;
    private String region;
    private String bucketName;
    private String uploadPath;
    private Credentials credentials;
    private Condition condition;
    private String volumeName;
    private String volumePrefix;

    /**
     * Constructs an empty {@code ApplyVolumeResponse}.
     */
    public ApplyVolumeResponse() {
    }

    /**
     * Constructs an {@code ApplyVolumeResponse} with the given volume information and credentials.
     *
     * @param endpoint     the storage endpoint of the applied volume
     * @param cloud        the cloud provider name
     * @param region       the region of the applied volume
     * @param bucketName   the bucket name of the applied volume
     * @param uploadPath   the path used for uploading files
     * @param credentials  the temporary credentials for accessing the volume
     * @param condition    the upload constraints of the volume
     * @param volumeName   the name of the applied volume
     * @param volumePrefix the prefix of the applied volume
     */
    public ApplyVolumeResponse(String endpoint, String cloud, String region, String bucketName, String uploadPath,
                               Credentials credentials, Condition condition, String volumeName, String volumePrefix) {
        this.endpoint = endpoint;
        this.cloud = cloud;
        this.region = region;
        this.bucketName = bucketName;
        this.uploadPath = uploadPath;
        this.credentials = credentials;
        this.condition = condition;
        this.volumeName = volumeName;
        this.volumePrefix = volumePrefix;
    }

    private ApplyVolumeResponse(ApplyVolumeResponseBuilder builder) {
        this.endpoint = builder.endpoint;
        this.cloud = builder.cloud;
        this.region = builder.region;
        this.bucketName = builder.bucketName;
        this.uploadPath = builder.uploadPath;
        this.credentials = builder.credentials;
        this.condition = builder.condition;
        this.volumeName = builder.volumeName;
        this.volumePrefix = builder.volumePrefix;
    }

    /**
     * Returns the storage endpoint of the applied volume.
     *
     * @return the storage endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the storage endpoint of the applied volume.
     *
     * @param endpoint the storage endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Returns the cloud provider name.
     *
     * @return the cloud provider name
     */
    public String getCloud() {
        return cloud;
    }

    /**
     * Sets the cloud provider name.
     *
     * @param cloud the cloud provider name
     */
    public void setCloud(String cloud) {
        this.cloud = cloud;
    }

    /**
     * Returns the region of the applied volume.
     *
     * @return the region of the applied volume
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region of the applied volume.
     *
     * @param region the region of the applied volume
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Returns the bucket name of the applied volume.
     *
     * @return the bucket name
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Sets the bucket name of the applied volume.
     *
     * @param bucketName the bucket name
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Returns the path used for uploading files to the volume.
     *
     * @return the upload path
     */
    public String getUploadPath() {
        return uploadPath;
    }

    /**
     * Sets the path used for uploading files to the volume.
     *
     * @param uploadPath the upload path
     */
    public void setUploadPath(String uploadPath) {
        this.uploadPath = uploadPath;
    }

    /**
     * Returns the temporary credentials for accessing the volume.
     *
     * @return the temporary credentials
     */
    public Credentials getCredentials() {
        return credentials;
    }

    /**
     * Sets the temporary credentials for accessing the volume.
     *
     * @param credentials the temporary credentials
     */
    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Returns the upload constraints enforced by the volume service.
     *
     * @return the upload constraints
     */
    public Condition getCondition() {
        return condition;
    }

    /**
     * Sets the upload constraints enforced by the volume service.
     *
     * @param condition the upload constraints
     */
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    /**
     * Returns the name of the applied volume.
     *
     * @return the volume name
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * Sets the name of the applied volume.
     *
     * @param volumeName the volume name
     */
    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    /**
     * Returns the prefix of the applied volume.
     *
     * @return the volume prefix
     */
    public String getVolumePrefix() {
        return volumePrefix;
    }

    /**
     * Sets the prefix of the applied volume.
     *
     * @param volumePrefix the volume prefix
     */
    public void setVolumePrefix(String volumePrefix) {
        this.volumePrefix = volumePrefix;
    }

    @Override
    public String toString() {
        return "ApplyVolumeResponse{" +
                ", endpoint='" + endpoint + '\'' +
                ", cloud='" + cloud + '\'' +
                ", region='" + region + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", uploadPath='" + uploadPath + '\'' +
                ", credentials=" + credentials +
                ", condition=" + condition +
                ", volumeName='" + volumeName + '\'' +
                ", volumePrefix='" + volumePrefix + '\'' +
                '}';
    }

    /**
     * Returns a new builder for an {@link ApplyVolumeResponse}.
     *
     * @return an {@code ApplyVolumeResponse} builder
     */
    public static ApplyVolumeResponseBuilder builder() {
        return new ApplyVolumeResponseBuilder();
    }

    /**
     * Builder for {@link ApplyVolumeResponse}.
     */
    public static class ApplyVolumeResponseBuilder {
        private String endpoint;
        private String cloud;
        private String region;
        private String bucketName;
        private String uploadPath;
        private Credentials credentials;
        private Condition condition;
        private String volumeName;
        private String volumePrefix;

        private ApplyVolumeResponseBuilder() {
            this.endpoint = "";
            this.cloud = "";
            this.region = "";
            this.bucketName = "";
            this.uploadPath = "";
            this.credentials = new Credentials();
            this.condition = new Condition();
            this.volumeName = "";
            this.volumePrefix = "";
        }

        /**
         * Sets the storage endpoint of the applied volume.
         *
         * @param endpoint the storage endpoint
         * @return this builder
         */
        public ApplyVolumeResponseBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Sets the cloud provider name.
         *
         * @param cloud the cloud provider name
         * @return this builder
         */
        public ApplyVolumeResponseBuilder cloud(String cloud) {
            this.cloud = cloud;
            return this;
        }

        /**
         * Sets the region of the applied volume.
         *
         * @param region the region of the applied volume
         * @return this builder
         */
        public ApplyVolumeResponseBuilder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the bucket name of the applied volume.
         *
         * @param bucketName the bucket name
         * @return this builder
         */
        public ApplyVolumeResponseBuilder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        /**
         * Sets the path used for uploading files to the volume.
         *
         * @param uploadPath the upload path
         * @return this builder
         */
        public ApplyVolumeResponseBuilder uploadPath(String uploadPath) {
            this.uploadPath = uploadPath;
            return this;
        }

        /**
         * Sets the temporary credentials for accessing the volume.
         *
         * @param credentials the temporary credentials
         * @return this builder
         */
        public ApplyVolumeResponseBuilder credentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /**
         * Sets the upload constraints enforced by the volume service.
         *
         * @param condition the upload constraints
         * @return this builder
         */
        public ApplyVolumeResponseBuilder condition(Condition condition) {
            this.condition = condition;
            return this;
        }

        /**
         * Sets the name of the applied volume.
         *
         * @param volumeName the volume name
         * @return this builder
         */
        public ApplyVolumeResponseBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Sets the prefix of the applied volume.
         *
         * @param volumePrefix the volume prefix
         * @return this builder
         */
        public ApplyVolumeResponseBuilder volumePrefix(String volumePrefix) {
            this.volumePrefix = volumePrefix;
            return this;
        }

        /**
         * Builds the {@link ApplyVolumeResponse} instance.
         *
         * @return the built {@code ApplyVolumeResponse}
         */
        public ApplyVolumeResponse build() {
            return new ApplyVolumeResponse(this);
        }
    }

    /**
     * Temporary credentials granted for accessing the applied volume.
     */
    public static class Credentials implements Serializable {
        private static final long serialVersionUID = 623702599895113789L;
        private String tmpAK;
        private String tmpSK;
        private String sessionToken;
        private String expireTime;

        /**
         * Constructs an empty {@code Credentials}.
         */
        public Credentials() {
        }

        /**
         * Constructs {@code Credentials} with the given temporary key pair and session token.
         *
         * @param tmpAK        the temporary access key
         * @param tmpSK        the temporary secret key
         * @param sessionToken the temporary session token
         * @param expireTime   the expiration time of the credentials
         */
        public Credentials(String tmpAK, String tmpSK, String sessionToken, String expireTime) {
            this.tmpAK = tmpAK;
            this.tmpSK = tmpSK;
            this.sessionToken = sessionToken;
            this.expireTime = expireTime;
        }

        private Credentials(CredentialsBuilder builder) {
            this.tmpAK = builder.tmpAK;
            this.tmpSK = builder.tmpSK;
            this.sessionToken = builder.sessionToken;
            this.expireTime = builder.expireTime;
        }

        /**
         * Returns the temporary access key.
         *
         * @return the temporary access key
         */
        public String getTmpAK() {
            return tmpAK;
        }

        /**
         * Sets the temporary access key.
         *
         * @param tmpAK the temporary access key
         */
        public void setTmpAK(String tmpAK) {
            this.tmpAK = tmpAK;
        }

        /**
         * Returns the temporary secret key.
         *
         * @return the temporary secret key
         */
        public String getTmpSK() {
            return tmpSK;
        }

        /**
         * Sets the temporary secret key.
         *
         * @param tmpSK the temporary secret key
         */
        public void setTmpSK(String tmpSK) {
            this.tmpSK = tmpSK;
        }

        /**
         * Returns the temporary session token.
         *
         * @return the temporary session token
         */
        public String getSessionToken() {
            return sessionToken;
        }

        /**
         * Sets the temporary session token.
         *
         * @param sessionToken the temporary session token
         */
        public void setSessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
        }

        /**
         * Returns the expiration time of the credentials.
         *
         * @return the expiration time
         */
        public String getExpireTime() {
            return expireTime;
        }

        /**
         * Sets the expiration time of the credentials.
         *
         * @param expireTime the expiration time
         */
        public void setExpireTime(String expireTime) {
            this.expireTime = expireTime;
        }

        @Override
        public String toString() {
            return "Credentials{" +
                    ", tmpAK='" + tmpAK + '\'' +
                    ", expireTime='" + expireTime + '\'' +
                    '}';
        }

        /**
         * Returns a new builder for {@link Credentials}.
         *
         * @return a {@code Credentials} builder
         */
        public static CredentialsBuilder builder() {
            return new CredentialsBuilder();
        }

        /**
         * Builder for {@link Credentials}.
         */
        public static class CredentialsBuilder {
            private String tmpAK;
            private String tmpSK;
            private String sessionToken;
            private String expireTime;

            private CredentialsBuilder() {
                this.tmpAK = "";
                this.tmpSK = "";
                this.sessionToken = "";
                this.expireTime = "";
            }

            /**
             * Sets the temporary access key.
             *
             * @param tmpAK the temporary access key
             * @return this builder
             */
            public CredentialsBuilder tmpAK(String tmpAK) {
                this.tmpAK = tmpAK;
                return this;
            }

            /**
             * Sets the temporary secret key.
             *
             * @param tmpSK the temporary secret key
             * @return this builder
             */
            public CredentialsBuilder tmpSK(String tmpSK) {
                this.tmpSK = tmpSK;
                return this;
            }

            /**
             * Sets the temporary session token.
             *
             * @param sessionToken the temporary session token
             * @return this builder
             */
            public CredentialsBuilder sessionToken(String sessionToken) {
                this.sessionToken = sessionToken;
                return this;
            }

            /**
             * Sets the expiration time of the credentials.
             *
             * @param expireTime the expiration time
             * @return this builder
             */
            public CredentialsBuilder expireTime(String expireTime) {
                this.expireTime = expireTime;
                return this;
            }

            /**
             * Builds the {@link Credentials} instance.
             *
             * @return the built {@code Credentials}
             */
            public Credentials build() {
                return new Credentials(this);
            }
        }
    }

    /**
     * Upload constraints enforced by the volume service.
     */
    public static class Condition implements Serializable {
        private static final long serialVersionUID = -2613029991242322109L;
        private Long maxContentLength;
        private Long maxFileNumber;

        /**
         * Constructs an empty {@code Condition}.
         */
        public Condition() {
        }

        /**
         * Constructs a {@code Condition} with the given maximum content length.
         *
         * @param maxContentLength the maximum content length of a single upload in bytes
         */
        public Condition(Long maxContentLength) {
            this.maxContentLength = maxContentLength;
        }

        private Condition(ConditionBuilder builder) {
            this.maxContentLength = builder.maxContentLength;
        }

        /**
         * Returns the maximum content length of a single upload in bytes.
         *
         * @return the maximum content length
         */
        public Long getMaxContentLength() {
            return maxContentLength;
        }

        /**
         * Returns the maximum number of files that can be uploaded to the volume.
         *
         * @return the maximum file number
         */
        public Long getMaxFileNumber() {
            return maxFileNumber;
        }

        /**
         * Sets the maximum content length of a single upload in bytes.
         *
         * @param maxContentLength the maximum content length
         */
        public void setMaxContentLength(Long maxContentLength) {
            this.maxContentLength = maxContentLength;
        }

        @Override
        public String toString() {
            return "Condition{" +
                    ", maxContentLength=" + maxContentLength +
                    ", maxFileNumber=" + maxFileNumber +
                    '}';
        }

        /**
         * Returns a new builder for {@link Condition}.
         *
         * @return a {@code Condition} builder
         */
        public static ConditionBuilder builder() {
            return new ConditionBuilder();
        }

        /**
         * Builder for {@link Condition}.
         */
        public static class ConditionBuilder {
            private Long maxContentLength;

            private ConditionBuilder() {
                this.maxContentLength = 0L;
            }

            /**
             * Sets the maximum content length of a single upload in bytes.
             *
             * @param maxContentLength the maximum content length
             * @return this builder
             */
            public ConditionBuilder maxContentLength(Long maxContentLength) {
                this.maxContentLength = maxContentLength;
                return this;
            }

            /**
             * Builds the {@link Condition} instance.
             *
             * @return the built {@code Condition}
             */
            public Condition build() {
                return new Condition(this);
            }
        }
    }
}
