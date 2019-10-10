package io.milvus.client;

import javax.annotation.Nonnull;

public class DeleteByRangeParam {
    private final DateRange dateRange;
    private final String tableName;
    private final long timeout;

    public static final class Builder {
        // Required parameters
        private final DateRange dateRange;
        private final String tableName;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        public Builder(DateRange dateRange, String tableName) {
            this.dateRange = dateRange;
            this.tableName = tableName;
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public DeleteByRangeParam build() {
            return new DeleteByRangeParam(this);
        }
    }

    private DeleteByRangeParam(@Nonnull Builder builder) {
        this.dateRange = builder.dateRange;
        this.tableName = builder.tableName;
        this.timeout = builder.timeout;
    }

    public DateRange getDateRange() {
        return dateRange;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "DeleteByRangeParam {" +
                "dateRange = " + dateRange.toString() +
                ", tableName = '" + tableName + '\'' +
                ", timeout = " + timeout +
                '}';
    }
}
