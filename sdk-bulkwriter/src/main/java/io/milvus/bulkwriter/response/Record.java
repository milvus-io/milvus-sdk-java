package io.milvus.bulkwriter.response;

public class Record {
    private String collectionName;
    private String jobId;
    private String state;

    public Record() {
    }

    public Record(String collectionName, String jobId, String state) {
        this.collectionName = collectionName;
        this.jobId = jobId;
        this.state = state;
    }

    private Record(RecordBuilder builder) {
        this.collectionName = builder.collectionName;
        this.jobId = builder.jobId;
        this.state = builder.state;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "Record{" +
                "collectionName='" + collectionName + '\'' +
                ", jobId='" + jobId + '\'' +
                ", state='" + state + '\'' +
                '}';
    }

    public static RecordBuilder builder() {
        return new RecordBuilder();
    }

    public static class RecordBuilder {
        private String collectionName;
        private String jobId;
        private String state;

        private RecordBuilder() {
            this.collectionName = "";
            this.jobId = "";
            this.state = "";
        }

        public RecordBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RecordBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public RecordBuilder state(String state) {
            this.state = state;
            return this;
        }

        public Record build() {
            return new Record(this);
        }
    }
}
