package io.milvus.param;

import javax.annotation.Nonnull;

public class DeleteParam {
    private final String dbName;
    private final String collectionName;
    private final String partitionName;

    private DeleteParam(@Nonnull Builder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public static class Builder {
        private String dbName = "";
        private String collectionName;
        private String partitionName ;

        private Builder() {
        }

        public static Builder nweBuilder() {
            return new Builder();
        }

        public Builder setPartitionName(@Nonnull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder setDbName(@Nonnull String dbName){
            this.dbName = dbName;
            return this;
        }

        public Builder setCollectionName(@Nonnull String collectionName){
            this.collectionName = collectionName;
            return this;
        }

        public DeleteParam build() {
            return new DeleteParam(this);
        }

    }
}
