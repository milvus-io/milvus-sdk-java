package io.milvus.client;

import javax.annotation.Nonnull;

public class IndexParam {

    private final String tableName;
    private final Index index;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final String tableName;

        // Optional parameters - initialized to default values
        private Index index;
        private long timeout = 86400;

        public Builder(@Nonnull String tableName) {
            this.tableName = tableName;
        }

        public Builder withIndex(Index index) {
            this.index = index;
            return this;
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public IndexParam build() {
            return new IndexParam(this);
        }
    }

    private IndexParam(@Nonnull Builder builder) {
        this.tableName = builder.tableName;
        this.index = builder.index;
        this.timeout = builder.timeout;
    }

    public String getTableName() {
        return tableName;
    }

    public Index getIndex() {
        return index;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return String.format("IndexParam = {tableName = %s, index = {indexType = %s, nList = %d}, timeout = %d}",
                                tableName, index.getIndexType().name(), index.getNList(), timeout);
    }
}
