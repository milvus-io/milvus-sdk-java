package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for <code>createIndex</code>
 */
public class CreateIndexParam {

    private final String tableName;
    private final Index index;
    private final long timeout;

    /**
     * Builder for <code>CreateIndexParam</code>
     */
    public static class Builder {
        // Required parameters
        private final String tableName;

        // Optional parameters - initialized to default values
        private Index index;
        private long timeout = 86400;

        /**
         * @param tableName table to create index on
         */
        public Builder(@Nonnull String tableName) {
            this.tableName = tableName;
        }

        /**
         * Optional. Default to Index{indexType = IndexType.FLAT, nList = 16384}
         * @param index a <code>Index</code> object
         * @return <code>Builder</code>
         * @see Index
         */
        public Builder withIndex(Index index) {
            this.index = index;
            return this;
        }

         /**
          * Optional. Sets the deadline from when the client RPC is set to when the response is picked up by the client.
          * Default to 86400s (1 day).
          * @param timeout in seconds
          * @return <code>Builder</code>
          */
        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public CreateIndexParam build() {
            return new CreateIndexParam(this);
        }
    }

    private CreateIndexParam(@Nonnull Builder builder) {
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
