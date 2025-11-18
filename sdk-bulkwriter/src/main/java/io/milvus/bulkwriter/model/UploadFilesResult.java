package io.milvus.bulkwriter.model;

public class UploadFilesResult {
    private String volumeName;
    private String path;

    public UploadFilesResult() {
    }

    public UploadFilesResult(String volumeName, String path) {
        this.volumeName = volumeName;
        this.path = path;
    }

    private UploadFilesResult(UploadFilesResultBuilder builder) {
        this.volumeName = builder.volumeName;
        this.path = builder.path;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "UploadFilesResult{" +
                "volumeName='" + volumeName + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static UploadFilesResultBuilder builder() {
        return new UploadFilesResultBuilder();
    }

    public static class UploadFilesResultBuilder {
        private String volumeName;
        private String path;

        private UploadFilesResultBuilder() {
            this.volumeName = "";
            this.path = "";
        }

        public UploadFilesResultBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public UploadFilesResultBuilder path(String path) {
            this.path = path;
            return this;
        }

        public UploadFilesResult build() {
            return new UploadFilesResult(this);
        }
    }
}
