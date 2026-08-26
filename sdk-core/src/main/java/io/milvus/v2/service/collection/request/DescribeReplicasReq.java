package io.milvus.v2.service.collection.request;

/**
 * Request parameters for the {@code describeReplicas} API.
 */
public class DescribeReplicasReq {
    private String collectionName;
    private String databaseName;

    private DescribeReplicasReq(DescribeReplicasReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String toString() {
        return "DescribeReplicasReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeReplicasReq}.
     *
     * @return the builder
     */
    public static DescribeReplicasReqBuilder builder() {
        return new DescribeReplicasReqBuilder();
    }

    public static class DescribeReplicasReqBuilder {
        private String collectionName;
        private String databaseName;

        private DescribeReplicasReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DescribeReplicasReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DescribeReplicasReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Builds a {@link DescribeReplicasReq} with the configured parameters.
         *
         * @return the request
         */
        public DescribeReplicasReq build() {
            return new DescribeReplicasReq(this);
        }
    }
}
