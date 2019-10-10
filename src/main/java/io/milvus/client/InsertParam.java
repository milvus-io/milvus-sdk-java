package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class InsertParam {
    private final String tableName;
    private final List<List<Float>> vectors;
    private final List<Long> vectorIds;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final String tableName;
        private final List<List<Float>> vectors;

        // Optional parameters - initialized to default values
        private List<Long> vectorIds = new ArrayList<>();
        private long timeout = 86400;

        public Builder(@Nonnull String tableName, @Nonnull List<List<Float>> vectors) {
            this.tableName = tableName;
            this.vectors = vectors;
        }

        public Builder withVectorIds(@Nonnull List<Long> vectorIds) {
            this.vectorIds = vectorIds;
            return this;
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public InsertParam build() {
            return new InsertParam(this);
        }
    }

    private InsertParam(@Nonnull Builder builder) {
        this.tableName = builder.tableName;
        this.vectors = builder.vectors;
        this.vectorIds = builder.vectorIds;
        this.timeout = builder.timeout;
    }

    public String getTableName() {
        return tableName;
    }

    public List<List<Float>> getVectors() {
        return vectors;
    }

    public List<Long> getVectorIds() {
        return vectorIds;
    }

    public long getTimeout() {
        return timeout;
    }
}
