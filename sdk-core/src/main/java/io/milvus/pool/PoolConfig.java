package io.milvus.pool;

import java.time.Duration;

/**
 * Configuration for the Milvus client pool, controlling the pool size, blocking and eviction
 * behavior.
 *
 * <p>Limits are expressed per key (endpoint) and for the pool as a whole. Default values match a
 * small client-side pool: one minimum idle client per key, a maximum of five idle clients per key
 * and a maximum of 1000 total clients.
 */
public class PoolConfig {
    private int maxIdlePerKey;
    private int minIdlePerKey;
    private int maxTotalPerKey;
    private int maxTotal;
    private boolean blockWhenExhausted;
    private Duration maxBlockWaitDuration;
    private Duration evictionPollingInterval;
    private Duration minEvictableIdleDuration;
    private boolean testOnBorrow;
    private boolean testOnReturn;

    private PoolConfig(Builder builder) {
        this.maxIdlePerKey = builder.maxIdlePerKey;
        this.minIdlePerKey = builder.minIdlePerKey;
        this.maxTotalPerKey = builder.maxTotalPerKey;
        this.maxTotal = builder.maxTotal;
        this.blockWhenExhausted = builder.blockWhenExhausted;
        this.maxBlockWaitDuration = builder.maxBlockWaitDuration;
        this.evictionPollingInterval = builder.evictionPollingInterval;
        this.minEvictableIdleDuration = builder.minEvictableIdleDuration;
        this.testOnBorrow = builder.testOnBorrow;
        this.testOnReturn = builder.testOnReturn;
    }

    /**
     * Creates a new {@code PoolConfig} builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    // Getters
    /**
     * Returns the maximum number of idle clients kept per key.
     *
     * @return the max idle clients per key
     */
    public int getMaxIdlePerKey() {
        return maxIdlePerKey;
    }

    /**
     * Returns the minimum number of idle clients kept per key.
     *
     * @return the min idle clients per key
     */
    public int getMinIdlePerKey() {
        return minIdlePerKey;
    }

    /**
     * Returns the maximum number of clients that can be created per key.
     *
     * @return the max total clients per key
     */
    public int getMaxTotalPerKey() {
        return maxTotalPerKey;
    }

    /**
     * Returns the maximum number of clients in the whole pool.
     *
     * @return the max total clients
     */
    public int getMaxTotal() {
        return maxTotal;
    }

    /**
     * Returns whether calls block when the pool is exhausted.
     *
     * @return {@code true} if calls block when the pool is exhausted
     */
    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    /**
     * Returns the maximum duration a call waits for an idle client when the pool is exhausted.
     *
     * @return the max block wait duration
     */
    public Duration getMaxBlockWaitDuration() {
        return maxBlockWaitDuration;
    }

    /**
     * Returns the interval between eviction runs of idle clients.
     *
     * @return the eviction polling interval
     */
    public Duration getEvictionPollingInterval() {
        return evictionPollingInterval;
    }

    /**
     * Returns the minimum idle duration after which an idle client is evictable.
     *
     * @return the min evictable idle duration
     */
    public Duration getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    /**
     * Returns whether clients are validated when borrowed from the pool.
     *
     * @return {@code true} if clients are tested on borrow
     */
    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    /**
     * Returns whether clients are validated when returned to the pool.
     *
     * @return {@code true} if clients are tested on return
     */
    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    // Setters
    /**
     * Sets the maximum number of idle clients kept per key.
     *
     * @param maxIdlePerKey the max idle clients per key
     */
    public void setMaxIdlePerKey(int maxIdlePerKey) {
        this.maxIdlePerKey = maxIdlePerKey;
    }

    /**
     * Sets the minimum number of idle clients kept per key.
     *
     * @param minIdlePerKey the min idle clients per key
     */
    public void setMinIdlePerKey(int minIdlePerKey) {
        this.minIdlePerKey = minIdlePerKey;
    }

    /**
     * Sets the maximum number of clients that can be created per key.
     *
     * @param maxTotalPerKey the max total clients per key
     */
    public void setMaxTotalPerKey(int maxTotalPerKey) {
        this.maxTotalPerKey = maxTotalPerKey;
    }

    /**
     * Sets the maximum number of clients in the whole pool.
     *
     * @param maxTotal the max total clients
     */
    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    /**
     * Sets whether calls block when the pool is exhausted.
     *
     * @param blockWhenExhausted {@code true} to block when the pool is exhausted
     */
    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    /**
     * Sets the maximum duration a call waits for an idle client when the pool is exhausted.
     *
     * @param maxBlockWaitDuration the max block wait duration
     */
    public void setMaxBlockWaitDuration(Duration maxBlockWaitDuration) {
        this.maxBlockWaitDuration = maxBlockWaitDuration;
    }

    /**
     * Sets the interval between eviction runs of idle clients.
     *
     * @param evictionPollingInterval the eviction polling interval
     */
    public void setEvictionPollingInterval(Duration evictionPollingInterval) {
        this.evictionPollingInterval = evictionPollingInterval;
    }

    /**
     * Sets the minimum idle duration after which an idle client is evictable.
     *
     * @param minEvictableIdleDuration the min evictable idle duration
     */
    public void setMinEvictableIdleDuration(Duration minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
    }

    /**
     * Sets whether clients are validated when borrowed from the pool.
     *
     * @param testOnBorrow {@code true} to test clients on borrow
     */
    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    /**
     * Sets whether clients are validated when returned to the pool.
     *
     * @param testOnReturn {@code true} to test clients on return
     */
    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    @Override
    public String toString() {
        return "PoolConfig{" +
                "maxIdlePerKey=" + maxIdlePerKey +
                ", minIdlePerKey=" + minIdlePerKey +
                ", maxTotalPerKey=" + maxTotalPerKey +
                ", maxTotal=" + maxTotal +
                ", blockWhenExhausted=" + blockWhenExhausted +
                ", maxBlockWaitDuration=" + maxBlockWaitDuration +
                ", evictionPollingInterval=" + evictionPollingInterval +
                ", minEvictableIdleDuration=" + minEvictableIdleDuration +
                ", testOnBorrow=" + testOnBorrow +
                ", testOnReturn=" + testOnReturn +
                '}';
    }

    /**
     * Builder for {@link PoolConfig}.
     */
    public static class Builder {
        private int minIdlePerKey = 1;
        private int maxIdlePerKey = 2;
        private int maxTotalPerKey = 5;
        private int maxTotal = 1000;
        private boolean blockWhenExhausted = true;
        private Duration maxBlockWaitDuration = Duration.ofSeconds(3L);
        private Duration evictionPollingInterval = Duration.ofSeconds(60L);
        private Duration minEvictableIdleDuration = Duration.ofSeconds(10L);
        private boolean testOnBorrow = false;
        private boolean testOnReturn = true;

        private Builder() {
        }

        /**
         * Sets the maximum number of idle clients kept per key.
         *
         * @param maxIdlePerKey the max idle clients per key
         * @return this builder
         */
        public Builder maxIdlePerKey(int maxIdlePerKey) {
            this.maxIdlePerKey = maxIdlePerKey;
            return this;
        }

        /**
         * Sets the minimum number of idle clients kept per key.
         *
         * @param minIdlePerKey the min idle clients per key
         * @return this builder
         */
        public Builder minIdlePerKey(int minIdlePerKey) {
            this.minIdlePerKey = minIdlePerKey;
            return this;
        }

        /**
         * Sets the maximum number of clients that can be created per key.
         *
         * @param maxTotalPerKey the max total clients per key
         * @return this builder
         */
        public Builder maxTotalPerKey(int maxTotalPerKey) {
            this.maxTotalPerKey = maxTotalPerKey;
            return this;
        }

        /**
         * Sets the maximum number of clients in the whole pool.
         *
         * @param maxTotal the max total clients
         * @return this builder
         */
        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets whether calls block when the pool is exhausted.
         *
         * @param blockWhenExhausted {@code true} to block when the pool is exhausted
         * @return this builder
         */
        public Builder blockWhenExhausted(boolean blockWhenExhausted) {
            this.blockWhenExhausted = blockWhenExhausted;
            return this;
        }

        /**
         * Sets the maximum duration a call waits for an idle client when the pool is exhausted.
         *
         * @param maxBlockWaitDuration the max block wait duration, must not be {@code null}
         * @return this builder
         * @throws IllegalArgumentException if {@code maxBlockWaitDuration} is {@code null}
         */
        public Builder maxBlockWaitDuration(Duration maxBlockWaitDuration) {
            if (maxBlockWaitDuration == null) {
                throw new IllegalArgumentException("maxBlockWaitDuration cannot be null");
            }
            this.maxBlockWaitDuration = maxBlockWaitDuration;
            return this;
        }

        /**
         * Sets the interval between eviction runs of idle clients.
         *
         * @param evictionPollingInterval the eviction polling interval, must not be {@code null}
         * @return this builder
         * @throws IllegalArgumentException if {@code evictionPollingInterval} is {@code null}
         */
        public Builder evictionPollingInterval(Duration evictionPollingInterval) {
            if (evictionPollingInterval == null) {
                throw new IllegalArgumentException("evictionPollingInterval cannot be null");
            }
            this.evictionPollingInterval = evictionPollingInterval;
            return this;
        }

        /**
         * Sets the minimum idle duration after which an idle client is evictable.
         *
         * @param minEvictableIdleDuration the min evictable idle duration, must not be {@code null}
         * @return this builder
         * @throws IllegalArgumentException if {@code minEvictableIdleDuration} is {@code null}
         */
        public Builder minEvictableIdleDuration(Duration minEvictableIdleDuration) {
            if (minEvictableIdleDuration == null) {
                throw new IllegalArgumentException("minEvictableIdleDuration cannot be null");
            }
            this.minEvictableIdleDuration = minEvictableIdleDuration;
            return this;
        }

        /**
         * Sets whether clients are validated when borrowed from the pool.
         *
         * @param testOnBorrow {@code true} to test clients on borrow
         * @return this builder
         */
        public Builder testOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
            return this;
        }

        /**
         * Sets whether clients are validated when returned to the pool.
         *
         * @param testOnReturn {@code true} to test clients on return
         * @return this builder
         */
        public Builder testOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
            return this;
        }

        /**
         * Builds a {@link PoolConfig} with the configured properties.
         *
         * @return the built {@code PoolConfig}
         */
        public PoolConfig build() {
            return new PoolConfig(this);
        }
    }
}
