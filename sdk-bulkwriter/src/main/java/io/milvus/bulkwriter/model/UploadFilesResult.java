package io.milvus.bulkwriter.model;

/**
 * The result of uploading files to a cloud storage volume.
 *
 * <p>It carries the name of the volume the files were uploaded to and the path of the
 * uploaded files within that volume.</p>
 */
public class UploadFilesResult {
    private String volumeName;
    private String path;

    /**
     * Constructs an empty {@code UploadFilesResult}.
     */
    public UploadFilesResult() {
    }

    /**
     * Constructs an {@code UploadFilesResult} with the given volume name and path.
     *
     * @param volumeName the name of the volume the files were uploaded to
     * @param path       the path of the uploaded files within the volume
     */
    public UploadFilesResult(String volumeName, String path) {
        this.volumeName = volumeName;
        this.path = path;
    }

    private UploadFilesResult(UploadFilesResultBuilder builder) {
        this.volumeName = builder.volumeName;
        this.path = builder.path;
    }

    /**
     * Returns the name of the volume the files were uploaded to.
     *
     * @return the volume name
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * Sets the name of the volume the files were uploaded to.
     *
     * @param volumeName the volume name
     */
    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    /**
     * Returns the path of the uploaded files within the volume.
     *
     * @return the upload path
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets the path of the uploaded files within the volume.
     *
     * @param path the upload path
     */
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

    /**
     * Returns a new builder for an {@link UploadFilesResult}.
     *
     * @return an {@code UploadFilesResult} builder
     */
    public static UploadFilesResultBuilder builder() {
        return new UploadFilesResultBuilder();
    }

    /**
     * Builder for {@link UploadFilesResult}.
     */
    public static class UploadFilesResultBuilder {
        private String volumeName;
        private String path;

        private UploadFilesResultBuilder() {
            this.volumeName = "";
            this.path = "";
        }

        /**
         * Sets the name of the volume the files were uploaded to.
         *
         * @param volumeName the volume name
         * @return this builder
         */
        public UploadFilesResultBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Sets the path of the uploaded files within the volume.
         *
         * @param path the upload path
         * @return this builder
         */
        public UploadFilesResultBuilder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builds the {@link UploadFilesResult} instance.
         *
         * @return the built {@code UploadFilesResult}
         */
        public UploadFilesResult build() {
            return new UploadFilesResult(this);
        }
    }
}
