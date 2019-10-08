package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class SearchParam {

    private final String tableName;
    private final List<List<Float>> queryVectors;
    private final List<DateRange> queryRanges;
    private final long topK;
    private final long nProbe;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final String tableName;
        private final List<List<Float>> queryVectors;

        // Optional parameters - initialized to default values
        private List<DateRange> queryRanges = new ArrayList<>();
        private long topK = 1;
        private long nProbe = 10;
        private long timeout = 10;

        public Builder(String tableName, List<List<Float>> queryVectors) {
            this.tableName = tableName;
            this.queryVectors = queryVectors;
        }

        public Builder withDateRanges(List<DateRange> queryRanges) {
            this.queryRanges = queryRanges;
            return this;
        }

        public Builder withTopK(long topK) {
            this.topK = topK;
            return this;
        }

        public Builder withNProbe(long nProbe) {
            this.nProbe = nProbe;
            return this;
        }

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
        this.queryRanges = builder.queryRanges;
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

    public List<DateRange> getQueryRanges() {
        return queryRanges;
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
