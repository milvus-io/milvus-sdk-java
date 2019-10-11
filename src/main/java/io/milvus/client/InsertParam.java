package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains parameters for <code>insert</code>
 */
public class InsertParam {
    private final String tableName;
    private final List<List<Float>> vectors;
    private final List<Long> vectorIds;
    private final long timeout;

    /**
     * Builder for <code>InsertParam</code>
     */
    public static class Builder {
        // Required parameters
        private final String tableName;
        private final List<List<Float>> vectors;

        // Optional parameters - initialized to default values
        private List<Long> vectorIds = new ArrayList<>();
        private long timeout = 86400;

        /**
         * @param tableName table to insert vectors to
         * @param vectors a <code>List</code> of vectors to insert. Each inner <code>List</code> represents a vector.
         */
        public Builder(@Nonnull String tableName, @Nonnull List<List<Float>> vectors) {
            this.tableName = tableName;
            this.vectors = vectors;
        }

        /**
         * Optional. Default to an empty <code>ArrayList</code>
         * @param vectorIds a <code>List</code> of ids associated with the vectors to insert
         * @return <code>Builder</code>
         */
        public Builder withVectorIds(@Nonnull List<Long> vectorIds) {
            this.vectorIds = vectorIds;
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
