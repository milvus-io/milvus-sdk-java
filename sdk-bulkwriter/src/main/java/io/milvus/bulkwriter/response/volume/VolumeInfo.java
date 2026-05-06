package io.milvus.bulkwriter.response.volume;

public class VolumeInfo {
    private String volumeName;
    private String type;
    private String regionId;
    private String storageIntegrationId;
    private String path;
    private String status;
    private String createTime;

    public VolumeInfo() {
    }

    public VolumeInfo(String volumeName) {
        this.volumeName = volumeName;
    }

    private VolumeInfo(VolumeInfoBuilder builder) {
        this.volumeName = builder.volumeName;
        this.type = builder.type;
        this.regionId = builder.regionId;
        this.storageIntegrationId = builder.storageIntegrationId;
        this.path = builder.path;
        this.status = builder.status;
        this.createTime = builder.createTime;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getStorageIntegrationId() {
        return storageIntegrationId;
    }

    public void setStorageIntegrationId(String storageIntegrationId) {
        this.storageIntegrationId = storageIntegrationId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "VolumeInfo{" +
                "volumeName='" + volumeName + '\'' +
                ", type='" + type + '\'' +
                ", regionId='" + regionId + '\'' +
                ", storageIntegrationId='" + storageIntegrationId + '\'' +
                ", path='" + path + '\'' +
                ", status='" + status + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }

    public static VolumeInfoBuilder builder() {
        return new VolumeInfoBuilder();
    }

    public static class VolumeInfoBuilder {
        private String volumeName;
        private String type;
        private String regionId;
        private String storageIntegrationId;
        private String path;
        private String status;
        private String createTime;

        private VolumeInfoBuilder() {
            this.volumeName = "";
        }

        public VolumeInfoBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public VolumeInfoBuilder type(String type) {
            this.type = type;
            return this;
        }

        public VolumeInfoBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        public VolumeInfoBuilder storageIntegrationId(String storageIntegrationId) {
            this.storageIntegrationId = storageIntegrationId;
            return this;
        }

        public VolumeInfoBuilder path(String path) {
            this.path = path;
            return this;
        }

        public VolumeInfoBuilder status(String status) {
            this.status = status;
            return this;
        }

        public VolumeInfoBuilder createTime(String createTime) {
            this.createTime = createTime;
            return this;
        }

        public VolumeInfo build() {
            return new VolumeInfo(this);
        }
    }
}
