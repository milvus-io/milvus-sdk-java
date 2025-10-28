package io.milvus.v2.service.collection.request;

public class DescribeReplicasReq {
    private String collectionName;
    private String databaseName;

    private DescribeReplicasReq(DescribeReplicasReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

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

    public static DescribeReplicasReqBuilder builder() {
        return new DescribeReplicasReqBuilder();
    }

    public static class DescribeReplicasReqBuilder {
        private String collectionName;
        private String databaseName;

        private DescribeReplicasReqBuilder() {
        }

        public DescribeReplicasReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DescribeReplicasReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DescribeReplicasReq build() {
            return new DescribeReplicasReq(this);
        }
    }
}
