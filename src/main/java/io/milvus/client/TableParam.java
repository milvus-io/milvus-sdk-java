package io.milvus.client;

import javax.annotation.Nonnull;

public class TableParam {
    private final String tableName;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final String tableName;

        // Optional parameters - initialized to default values
        private long timeout = 10;

        public Builder(String tableName) {
            this.tableName = tableName;
        }

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
