package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for <code>hasTable</code>, <code>dropTable</code>, <code>describeTable</code>,
 * <code>getTableRowCount</code>, <code>preloadTable</code>, <code>describeIndex</code> and <code>dropIndex</code>
 */
public class TableParam {
    private final String tableName;
    private final long timeout;

    /**
     * Builder for <code>TableParam</code>
     */
    public static class Builder {
        // Required parameters
        private final String tableName;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        /**
         * @param tableName table name
         */
        public Builder(@Nonnull String tableName) {
            this.tableName = tableName;
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

        public TableParam build() {
            return new TableParam(this);
        }
    }

    private TableParam(@Nonnull Builder builder) {
        this.tableName = builder.tableName;
        this.timeout = builder.timeout;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "TableParam {" +
                "tableName = '" + tableName + '\'' +
                ", timeout = " + timeout +
                '}';
    }
}
