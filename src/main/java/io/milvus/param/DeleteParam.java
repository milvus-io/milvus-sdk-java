package io.milvus.param;

import javax.annotation.Nonnull;

public class DeleteParam {
    private final String dbName;
    private final String collectionName;
    private final String partitionName;
    private final String expr;

    private DeleteParam(@Nonnull Builder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.expr = builder.expr;;
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

    public String getExpr() {
        return expr;
    }

    public static class Builder {
        private String dbName = "";
        private String collectionName;
        private String partitionName ;
        private String expr;

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


        public Builder setExpr(String expr) {
            this.expr = expr;
            return this;
        }

        public DeleteParam build() {
            return new DeleteParam(this);
        }

    }
}
