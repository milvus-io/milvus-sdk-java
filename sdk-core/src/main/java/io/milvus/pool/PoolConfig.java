package io.milvus.pool;

import java.time.Duration;

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

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public int getMaxIdlePerKey() {
        return maxIdlePerKey;
    }

    public int getMinIdlePerKey() {
        return minIdlePerKey;
    }

    public int getMaxTotalPerKey() {
        return maxTotalPerKey;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public Duration getMaxBlockWaitDuration() {
        return maxBlockWaitDuration;
    }

    public Duration getEvictionPollingInterval() {
        return evictionPollingInterval;
    }

    public Duration getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    // Setters
    public void setMaxIdlePerKey(int maxIdlePerKey) {
        this.maxIdlePerKey = maxIdlePerKey;
    }

    public void setMinIdlePerKey(int minIdlePerKey) {
        this.minIdlePerKey = minIdlePerKey;
    }

    public void setMaxTotalPerKey(int maxTotalPerKey) {
        this.maxTotalPerKey = maxTotalPerKey;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    public void setMaxBlockWaitDuration(Duration maxBlockWaitDuration) {
        this.maxBlockWaitDuration = maxBlockWaitDuration;
    }

    public void setEvictionPollingInterval(Duration evictionPollingInterval) {
        this.evictionPollingInterval = evictionPollingInterval;
    }

    public void setMinEvictableIdleDuration(Duration minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

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

        public Builder maxIdlePerKey(int maxIdlePerKey) {
            this.maxIdlePerKey = maxIdlePerKey;
            return this;
        }

        public Builder minIdlePerKey(int minIdlePerKey) {
            this.minIdlePerKey = minIdlePerKey;
            return this;
        }

        public Builder maxTotalPerKey(int maxTotalPerKey) {
            this.maxTotalPerKey = maxTotalPerKey;
            return this;
        }

        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder blockWhenExhausted(boolean blockWhenExhausted) {
            this.blockWhenExhausted = blockWhenExhausted;
            return this;
        }

        public Builder maxBlockWaitDuration(Duration maxBlockWaitDuration) {
            if (maxBlockWaitDuration == null) {
                throw new IllegalArgumentException("maxBlockWaitDuration cannot be null");
            }
            this.maxBlockWaitDuration = maxBlockWaitDuration;
            return this;
        }

        public Builder evictionPollingInterval(Duration evictionPollingInterval) {
            if (evictionPollingInterval == null) {
                throw new IllegalArgumentException("evictionPollingInterval cannot be null");
            }
            this.evictionPollingInterval = evictionPollingInterval;
            return this;
        }

        public Builder minEvictableIdleDuration(Duration minEvictableIdleDuration) {
            if (minEvictableIdleDuration == null) {
                throw new IllegalArgumentException("minEvictableIdleDuration cannot be null");
            }
            this.minEvictableIdleDuration = minEvictableIdleDuration;
            return this;
        }

        public Builder testOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
            return this;
        }

        public Builder testOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
            return this;
        }

        public PoolConfig build() {
            return new PoolConfig(this);
        }
    }
}
