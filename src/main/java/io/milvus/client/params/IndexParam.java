package io.milvus.client.params;

public class IndexParam {

    private final String tableName;
    private final Index index;

    public static class Builder {
        // Required parameters
        private final String tableName;

        // Optional parameters - initialized to default values
        private Index index;

        public Builder(String tableName) {
            this.tableName = tableName;
        }

        public Builder setIndex(Index indexToSet) {
            index = indexToSet;
            return this;
        }

        public IndexParam build() {
            return new IndexParam(this);
        }
    }

    private IndexParam(Builder builder) {
        this.tableName = builder.tableName;
        this.index = builder.index;
    }

    public String getTableName() {
        return tableName;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("IndexParam {tableName = %s, index = {indexType = %s, nList = %d}",
                                tableName, index.getIndexType().name(), index.getnNList());
    }
}
