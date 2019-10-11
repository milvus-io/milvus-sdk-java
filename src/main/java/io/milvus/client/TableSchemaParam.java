package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for <code>createTable</code>
 */
public class TableSchemaParam {
    private final TableSchema tableSchema;
    private final long timeout;

    /**
     * Builder for <code>TableSchemaParam</code>
     */
    public static class Builder {
        // Required parameters
        private final TableSchema tableSchema;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        /**
         * @param tableSchema a <code>TableSchema</code> object
         * @see TableSchema
         */
        public Builder(@Nonnull TableSchema tableSchema) {
            this.tableSchema = tableSchema;
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

        public TableSchemaParam build() {
            return new TableSchemaParam(this);
        }
    }

    private TableSchemaParam(@Nonnull Builder builder) {
        this.tableSchema = builder.tableSchema;
        this.timeout = builder.timeout;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "CreateTableParam {" +
                tableSchema +
                ", timeout = " + timeout +
                '}';
    }
}
