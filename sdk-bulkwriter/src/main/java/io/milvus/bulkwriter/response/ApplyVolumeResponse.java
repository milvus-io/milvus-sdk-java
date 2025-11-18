package io.milvus.bulkwriter.response;

import java.io.Serializable;


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

    public ApplyVolumeResponse() {
    }

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

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getCloud() {
        return cloud;
    }

    public void setCloud(String cloud) {
        this.cloud = cloud;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public void setUploadPath(String uploadPath) {
        this.uploadPath = uploadPath;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public String getVolumePrefix() {
        return volumePrefix;
    }

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

    public static ApplyVolumeResponseBuilder builder() {
        return new ApplyVolumeResponseBuilder();
    }

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

        public ApplyVolumeResponseBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public ApplyVolumeResponseBuilder cloud(String cloud) {
            this.cloud = cloud;
            return this;
        }

        public ApplyVolumeResponseBuilder region(String region) {
            this.region = region;
            return this;
        }

        public ApplyVolumeResponseBuilder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public ApplyVolumeResponseBuilder uploadPath(String uploadPath) {
            this.uploadPath = uploadPath;
            return this;
        }

        public ApplyVolumeResponseBuilder credentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }

        public ApplyVolumeResponseBuilder condition(Condition condition) {
            this.condition = condition;
            return this;
        }

        public ApplyVolumeResponseBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public ApplyVolumeResponseBuilder volumePrefix(String volumePrefix) {
            this.volumePrefix = volumePrefix;
            return this;
        }

        public ApplyVolumeResponse build() {
            return new ApplyVolumeResponse(this);
        }
    }

    public static class Credentials implements Serializable {
        private static final long serialVersionUID = 623702599895113789L;
        private String tmpAK;
        private String tmpSK;
        private String sessionToken;
        private String expireTime;

        public Credentials() {
        }

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

        public String getTmpAK() {
            return tmpAK;
        }

        public void setTmpAK(String tmpAK) {
            this.tmpAK = tmpAK;
        }

        public String getTmpSK() {
            return tmpSK;
        }

        public void setTmpSK(String tmpSK) {
            this.tmpSK = tmpSK;
        }

        public String getSessionToken() {
            return sessionToken;
        }

        public void setSessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
        }

        public String getExpireTime() {
            return expireTime;
        }

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

        public static CredentialsBuilder builder() {
            return new CredentialsBuilder();
        }

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

            public CredentialsBuilder tmpAK(String tmpAK) {
                this.tmpAK = tmpAK;
                return this;
            }

            public CredentialsBuilder tmpSK(String tmpSK) {
                this.tmpSK = tmpSK;
                return this;
            }

            public CredentialsBuilder sessionToken(String sessionToken) {
                this.sessionToken = sessionToken;
                return this;
            }

            public CredentialsBuilder expireTime(String expireTime) {
                this.expireTime = expireTime;
                return this;
            }

            public Credentials build() {
                return new Credentials(this);
            }
        }
    }

    public static class Condition implements Serializable {
        private static final long serialVersionUID = -2613029991242322109L;
        private Long maxContentLength;
        private Long maxFileNumber;

        public Condition() {
        }

        public Condition(Long maxContentLength) {
            this.maxContentLength = maxContentLength;
        }

        private Condition(ConditionBuilder builder) {
            this.maxContentLength = builder.maxContentLength;
        }

        public Long getMaxContentLength() {
            return maxContentLength;
        }

        public Long getMaxFileNumber() {
            return maxFileNumber;
        }

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

        public static ConditionBuilder builder() {
            return new ConditionBuilder();
        }

        public static class ConditionBuilder {
            private Long maxContentLength;

            private ConditionBuilder() {
                this.maxContentLength = 0L;
            }

            public ConditionBuilder maxContentLength(Long maxContentLength) {
                this.maxContentLength = maxContentLength;
                return this;
            }

            public Condition build() {
                return new Condition(this);
            }
        }
    }
}
