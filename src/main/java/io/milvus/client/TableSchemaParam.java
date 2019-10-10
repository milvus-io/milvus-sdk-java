package io.milvus.client;

import javax.annotation.Nonnull;

public class TableSchemaParam {
    private final TableSchema tableSchema;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final TableSchema tableSchema;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        public Builder(@Nonnull TableSchema tableSchema) {
            this.tableSchema = tableSchema;
        }

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
