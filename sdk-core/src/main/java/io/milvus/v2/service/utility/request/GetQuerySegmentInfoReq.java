package io.milvus.v2.service.utility.request;

public class GetQuerySegmentInfoReq {
    private String databaseName;
    private String collectionName;

    private GetQuerySegmentInfoReq(GetQuerySegmentInfoReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
    }

    public static GetQuerySegmentInfoReqBuilder builder() {
        return new GetQuerySegmentInfoReqBuilder();
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
        return "GetQuerySegmentInfoReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    public static class GetQuerySegmentInfoReqBuilder {
        private String databaseName;
        private String collectionName;

        public GetQuerySegmentInfoReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public GetQuerySegmentInfoReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetQuerySegmentInfoReq build() {
            return new GetQuerySegmentInfoReq(this);
        }
    }
}
