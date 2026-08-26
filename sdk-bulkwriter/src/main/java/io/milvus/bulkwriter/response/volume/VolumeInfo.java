package io.milvus.bulkwriter.response.volume;

/**
 * Information about a cloud storage volume used by the BulkWriter.
 *
 * <p>It describes a volume's name, type, region, storage integration, path, status, and
 * creation time, and is returned by the volume describe and list APIs.</p>
 */
public class VolumeInfo {
    private String volumeName;
    private String type;
    private String regionId;
    private String storageIntegrationId;
    private String path;
    private String status;
    private String createTime;

    /**
     * Constructs an empty {@code VolumeInfo}.
     */
    public VolumeInfo() {
    }

    /**
     * Constructs a {@code VolumeInfo} with the given volume name.
     *
     * @param volumeName the name of the volume
     */
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

    /**
     * Returns the name of the volume.
     *
     * @return the volume name
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * Sets the name of the volume.
     *
     * @param volumeName the volume name
     */
    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    /**
     * Returns the type of the volume.
     *
     * @return the volume type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of the volume.
     *
     * @param type the volume type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the region ID where the volume resides.
     *
     * @return the region ID
     */
    public String getRegionId() {
        return regionId;
    }

    /**
     * Sets the region ID where the volume resides.
     *
     * @param regionId the region ID
     */
    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    /**
     * Returns the ID of the storage integration associated with the volume.
     *
     * @return the storage integration ID
     */
    public String getStorageIntegrationId() {
        return storageIntegrationId;
    }

    /**
     * Sets the ID of the storage integration associated with the volume.
     *
     * @param storageIntegrationId the storage integration ID
     */
    public void setStorageIntegrationId(String storageIntegrationId) {
        this.storageIntegrationId = storageIntegrationId;
    }

    /**
     * Returns the path of the volume within the object storage.
     *
     * @return the volume path
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets the path of the volume within the object storage.
     *
     * @param path the volume path
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Returns the current status of the volume.
     *
     * @return the volume status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the current status of the volume.
     *
     * @param status the volume status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Returns the creation time of the volume.
     *
     * @return the creation time
     */
    public String getCreateTime() {
        return createTime;
    }

    /**
     * Sets the creation time of the volume.
     *
     * @param createTime the creation time
     */
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

    /**
     * Returns a new builder for a {@link VolumeInfo}.
     *
     * @return a {@code VolumeInfo} builder
     */
    public static VolumeInfoBuilder builder() {
        return new VolumeInfoBuilder();
    }

    /**
     * Builder for {@link VolumeInfo}.
     */
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

        /**
         * Sets the name of the volume.
         *
         * @param volumeName the volume name
         * @return this builder
         */
        public VolumeInfoBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Sets the type of the volume.
         *
         * @param type the volume type
         * @return this builder
         */
        public VolumeInfoBuilder type(String type) {
            this.type = type;
            return this;
        }

        /**
         * Sets the region ID where the volume resides.
         *
         * @param regionId the region ID
         * @return this builder
         */
        public VolumeInfoBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        /**
         * Sets the ID of the storage integration associated with the volume.
         *
         * @param storageIntegrationId the storage integration ID
         * @return this builder
         */
        public VolumeInfoBuilder storageIntegrationId(String storageIntegrationId) {
            this.storageIntegrationId = storageIntegrationId;
            return this;
        }

        /**
         * Sets the path of the volume within the object storage.
         *
         * @param path the volume path
         * @return this builder
         */
        public VolumeInfoBuilder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Sets the current status of the volume.
         *
         * @param status the volume status
         * @return this builder
         */
        public VolumeInfoBuilder status(String status) {
            this.status = status;
            return this;
        }

        /**
         * Sets the creation time of the volume.
         *
         * @param createTime the creation time
         * @return this builder
         */
        public VolumeInfoBuilder createTime(String createTime) {
            this.createTime = createTime;
            return this;
        }

        /**
         * Builds the {@link VolumeInfo} instance.
         *
         * @return the built {@code VolumeInfo}
         */
        public VolumeInfo build() {
            return new VolumeInfo(this);
        }
    }
}
