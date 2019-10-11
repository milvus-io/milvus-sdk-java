package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains parameters for <code>search</code>
 */
public class SearchParam {

    private final String tableName;
    private final List<List<Float>> queryVectors;
    private final List<DateRange> dateRanges;
    private final long topK;
    private final long nProbe;
    private final long timeout;

    /**
     * Builder for <code>SearchParam</code>
     */
    public static class Builder {
        // Required parameters
        private final String tableName;
        private final List<List<Float>> queryVectors;

        // Optional parameters - initialized to default values
        private List<DateRange> dateRanges = new ArrayList<>();
        private long topK = 1024;
        private long nProbe = 20;
        private long timeout = 86400;

        /**
         * @param tableName table to search from
         * @param queryVectors a <code>List</code> of vectors to be queried. Each inner <code>List</code> represents a vector.
         */
        public Builder(@Nonnull String tableName, @Nonnull List<List<Float>> queryVectors) {
            this.tableName = tableName;
            this.queryVectors = queryVectors;
        }

        /**
         * Optional. Searches vectors in their corresponding date range. Default to an empty <code>ArrayList</code>
         * @param dateRanges a <code>List</code> of <code>DateRange</code> objects
         * @return <code>Builder</code>
         * @see DateRange
         */
        public Builder withDateRanges(@Nonnull List<DateRange> dateRanges) {
            this.dateRanges = dateRanges;
            return this;
        }

        /**
         * Optional. Limits search result to <code>topK</code>. Default to 1024.
         * @param topK a topK number
         * @return <code>Builder</code>
         */
        public Builder withTopK(long topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Optional. Default to 20.
         * @param nProbe a nProbe number
         * @return <code>Builder</code>
         */
        public Builder withNProbe(long nProbe) {
            this.nProbe = nProbe;
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

        public SearchParam build() {
            return new SearchParam(this);
        }
    }

    private SearchParam(@Nonnull Builder builder) {
        this.tableName = builder.tableName;
        this.queryVectors = builder.queryVectors;
        this.dateRanges = builder.dateRanges;
        this.nProbe = builder.nProbe;
        this.topK = builder.topK;
        this.timeout = builder.timeout;
    }

    public String getTableName() {
        return tableName;
    }

    public List<List<Float>> getQueryVectors() {
        return queryVectors;
    }

    public List<DateRange> getdateRanges() {
        return dateRanges;
    }

    public long getTopK() {
        return topK;
    }

    public long getNProbe() {
        return nProbe;
    }

    public long getTimeout() {
        return timeout;
    }
}
