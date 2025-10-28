package io.milvus.v2.service.utility.request;

public class GetPersistentSegmentInfoReq {
    private String databaseName;
    private String collectionName;

    private GetPersistentSegmentInfoReq(GetPersistentSegmentInfoReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
    }

    public static GetPersistentSegmentInfoReqBuilder builder() {
        return new GetPersistentSegmentInfoReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        return "GetPersistentSegmentInfoReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    public static class GetPersistentSegmentInfoReqBuilder {
        private String databaseName;
        private String collectionName;

        public GetPersistentSegmentInfoReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public GetPersistentSegmentInfoReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetPersistentSegmentInfoReq build() {
            return new GetPersistentSegmentInfoReq(this);
        }
    }
}
