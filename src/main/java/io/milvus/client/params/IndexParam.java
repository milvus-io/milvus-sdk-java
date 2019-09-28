package io.milvus.client.params;

import javax.annotation.Nonnull;

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

        public Builder withIndex(Index index) {
            this.index = index;
            return this;
        }

        public IndexParam build() {
            return new IndexParam(this);
        }
    }

    private IndexParam(@Nonnull Builder builder) {
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
        return String.format("IndexParam = {tableName = %s, index = {indexType = %s, nList = %d}",
                                tableName, index.getIndexType().name(), index.getNList());
    }
}
