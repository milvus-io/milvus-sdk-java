package io.milvus.bulkwriter.response.stage;

public class StageInfo {
    private String stageName;

    public StageInfo() {
    }

    public StageInfo(String stageName) {
        this.stageName = stageName;
    }

    private StageInfo(StageInfoBuilder builder) {
        this.stageName = builder.stageName;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    @Override
    public String toString() {
        return "StageInfo{" +
                "stageName='" + stageName + '\'' +
                '}';
    }

    public static StageInfoBuilder builder() {
        return new StageInfoBuilder();
    }

    public static class StageInfoBuilder {
        private String stageName;

        private StageInfoBuilder() {
            this.stageName = "";
        }

        public StageInfoBuilder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public StageInfo build() {
            return new StageInfo(this);
        }
    }
}
