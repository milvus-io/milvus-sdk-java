package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for <code>deleteByRange</code>
 */
public class DeleteByRangeParam {
    private final DateRange dateRange;
    private final String tableName;
    private final long timeout;

    /**
     * Builder for <code>DeleteByRangeParam</code>
     */
    public static final class Builder {
        // Required parameters
        private final DateRange dateRange;
        private final String tableName;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        /**
         * @param dateRange a <code>DateRange</code> object
         * @param tableName table to delete from
         * @see DateRange
         */
        public Builder(@Nonnull DateRange dateRange, @Nonnull String tableName) {
            this.dateRange = dateRange;
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
