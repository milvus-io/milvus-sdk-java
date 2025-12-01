package io.milvus.bulkwriter.response.volume;

public class VolumeInfo {
    private String volumeName;

    public VolumeInfo() {
    }

    public VolumeInfo(String volumeName) {
        this.volumeName = volumeName;
    }

    private VolumeInfo(VolumeInfoBuilder builder) {
        this.volumeName = builder.volumeName;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    @Override
    public String toString() {
        return "VolumeInfo{" +
                "volumeName='" + volumeName + '\'' +
                '}';
    }

    public static VolumeInfoBuilder builder() {
        return new VolumeInfoBuilder();
    }

    public static class VolumeInfoBuilder {
        private String volumeName;

        private VolumeInfoBuilder() {
            this.volumeName = "";
        }

        public VolumeInfoBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public VolumeInfo build() {
            return new VolumeInfo(this);
        }
    }
}
