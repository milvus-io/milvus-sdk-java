package io.milvus.bulkwriter.model;

public class UploadFilesResult {
    private String stageName;
    private String path;

    public UploadFilesResult() {
    }

    public UploadFilesResult(String stageName, String path) {
        this.stageName = stageName;
        this.path = path;
    }

    private UploadFilesResult(UploadFilesResultBuilder builder) {
        this.stageName = builder.stageName;
        this.path = builder.path;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
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
                "stageName='" + stageName + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static UploadFilesResultBuilder builder() {
        return new UploadFilesResultBuilder();
    }

    public static class UploadFilesResultBuilder {
        private String stageName;
        private String path;

        private UploadFilesResultBuilder() {
            this.stageName = "";
            this.path = "";
        }

        public UploadFilesResultBuilder stageName(String stageName) {
            this.stageName = stageName;
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
