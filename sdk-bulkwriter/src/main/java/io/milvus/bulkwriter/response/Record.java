package io.milvus.bulkwriter.response;

/**
 * A record of a bulk import job returned by the list import jobs API.
 *
 * <p>It describes a single import job through its collection, unique job ID, and state.</p>
 */
public class Record {
    private String collectionName;
    private String jobId;
    private String state;

    /**
     * Constructs an empty {@code Record}.
     */
    public Record() {
    }

    /**
     * Constructs a {@code Record} with the given collection name, job ID, and state.
     *
     * @param collectionName the name of the collection targeted by the import job
     * @param jobId          the unique ID of the import job
     * @param state          the current state of the import job
     */
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

    /**
     * Returns the name of the collection targeted by the import job.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection targeted by the import job.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the unique ID of the import job.
     *
     * @return the import job ID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Sets the unique ID of the import job.
     *
     * @param jobId the import job ID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns the current state of the import job.
     *
     * @return the import job state
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the current state of the import job.
     *
     * @param state the import job state
     */
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

    /**
     * Returns a new builder for a {@link Record}.
     *
     * @return a {@code Record} builder
     */
    public static RecordBuilder builder() {
        return new RecordBuilder();
    }

    /**
     * Builder for {@link Record}.
     */
    public static class RecordBuilder {
        private String collectionName;
        private String jobId;
        private String state;

        private RecordBuilder() {
            this.collectionName = "";
            this.jobId = "";
            this.state = "";
        }

        /**
         * Sets the name of the collection targeted by the import job.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public RecordBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the unique ID of the import job.
         *
         * @param jobId the import job ID
         * @return this builder
         */
        public RecordBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the current state of the import job.
         *
         * @param state the import job state
         * @return this builder
         */
        public RecordBuilder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Builds the {@link Record} instance.
         *
         * @return the built {@code Record}
         */
        public Record build() {
            return new Record(this);
        }
    }
}
