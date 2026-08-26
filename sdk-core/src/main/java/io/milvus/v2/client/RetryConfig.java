/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.client;

/**
 * Configuration for the retry policy applied to Milvus RPC calls.
 * <p>
 * Use {@link #builder()} to create a configuration, then pass it to
 * {@link MilvusClientV2#withRetry(RetryConfig)}.
 */
public class RetryConfig {
    private int maxRetryTimes = 75;
    private long initialBackOffMs = 10;
    private long maxBackOffMs = 3000;
    private int backOffMultiplier = 3;
    private boolean retryOnRateLimit = true;
    private long maxRetryTimeoutMs = 0;

    // Constructor for builder pattern
    private RetryConfig(RetryConfigBuilder builder) {
        this(builder.maxRetryTimes, builder.initialBackOffMs, builder.maxBackOffMs,
                builder.backOffMultiplier, builder.retryOnRateLimit, builder.maxRetryTimeoutMs);
    }

    private RetryConfig(int maxRetryTimes, long initialBackOffMs, long maxBackOffMs,
                        int backOffMultiplier, boolean retryOnRateLimit, long maxRetryTimeoutMs) {
        requireNonNegative("initialBackOffMs", initialBackOffMs);
        requireNonNegative("maxBackOffMs", maxBackOffMs);
        requireAtLeastOne("backOffMultiplier", backOffMultiplier);
        this.maxRetryTimes = maxRetryTimes;
        this.initialBackOffMs = initialBackOffMs;
        this.maxBackOffMs = maxBackOffMs;
        this.backOffMultiplier = backOffMultiplier;
        this.retryOnRateLimit = retryOnRateLimit;
        this.maxRetryTimeoutMs = maxRetryTimeoutMs;
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    name + " cannot be negative: " + value);
        }
    }

    private static void requireAtLeastOne(String name, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(
                    name + " must be at least 1: " + value);
        }
    }

    /**
     * Returns a builder for {@link RetryConfig}.
     *
     * @return a retry configuration builder
     */
    public static RetryConfigBuilder builder() {
        return new RetryConfigBuilder();
    }

    // Getters
    /**
     * Returns the maximum number of retry attempts.
     *
     * @return the maximum number of retries
     */
    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    /**
     * Returns the initial back-off delay in milliseconds.
     *
     * @return the initial back-off in milliseconds
     */
    public long getInitialBackOffMs() {
        return initialBackOffMs;
    }

    /**
     * Returns the maximum back-off delay in milliseconds.
     *
     * @return the maximum back-off in milliseconds
     */
    public long getMaxBackOffMs() {
        return maxBackOffMs;
    }

    /**
     * Returns the multiplier applied to the back-off delay after each retry.
     *
     * @return the back-off multiplier
     */
    public int getBackOffMultiplier() {
        return backOffMultiplier;
    }

    /**
     * Returns whether retries are enabled when the server reports a rate limit.
     *
     * @return true if rate-limit errors are retried
     */
    public boolean isRetryOnRateLimit() {
        return retryOnRateLimit;
    }

    /**
     * Returns the overall retry timeout in milliseconds. A value of zero means no timeout.
     *
     * @return the retry timeout in milliseconds
     */
    public long getMaxRetryTimeoutMs() {
        return maxRetryTimeoutMs;
    }

    // Setters
    /**
     * Sets the maximum number of retry attempts.
     *
     * @param maxRetryTimes the maximum number of retries
     */
    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    /**
     * Sets the initial back-off delay in milliseconds.
     *
     * @param initialBackOffMs the initial back-off in milliseconds
     */
    public void setInitialBackOffMs(long initialBackOffMs) {
        requireNonNegative("initialBackOffMs", initialBackOffMs);
        this.initialBackOffMs = initialBackOffMs;
    }

    /**
     * Sets the maximum back-off delay in milliseconds.
     *
     * @param maxBackOffMs the maximum back-off in milliseconds
     */
    public void setMaxBackOffMs(long maxBackOffMs) {
        requireNonNegative("maxBackOffMs", maxBackOffMs);
        this.maxBackOffMs = maxBackOffMs;
    }

    /**
     * Sets the multiplier applied to the back-off delay after each retry.
     *
     * @param backOffMultiplier the back-off multiplier
     */
    public void setBackOffMultiplier(int backOffMultiplier) {
        requireAtLeastOne("backOffMultiplier", backOffMultiplier);
        this.backOffMultiplier = backOffMultiplier;
    }

    /**
     * Sets whether retries are enabled when the server reports a rate limit.
     *
     * @param retryOnRateLimit whether rate-limit errors are retried
     */
    public void setRetryOnRateLimit(boolean retryOnRateLimit) {
        this.retryOnRateLimit = retryOnRateLimit;
    }

    /**
     * Sets the overall retry timeout in milliseconds. A value of zero means no timeout.
     *
     * @param maxRetryTimeoutMs the retry timeout in milliseconds
     */
    public void setMaxRetryTimeoutMs(long maxRetryTimeoutMs) {
        this.maxRetryTimeoutMs = maxRetryTimeoutMs;
    }

    @Override
    public String toString() {
        return "RetryConfig{" +
                "maxRetryTimes=" + maxRetryTimes +
                ", initialBackOffMs=" + initialBackOffMs +
                ", maxBackOffMs=" + maxBackOffMs +
                ", backOffMultiplier=" + backOffMultiplier +
                ", retryOnRateLimit=" + retryOnRateLimit +
                ", maxRetryTimeoutMs=" + maxRetryTimeoutMs +
                '}';
    }

    // Builder class with public access modifier
    public static class RetryConfigBuilder {
        private int maxRetryTimes = 75;
        private long initialBackOffMs = 10;
        private long maxBackOffMs = 3000;
        private int backOffMultiplier = 3;
        private boolean retryOnRateLimit = true;
        private long maxRetryTimeoutMs = 0;

        /**
         * Sets the maximum number of retry attempts.
         *
         * @param maxRetryTimes the maximum number of retries
         * @return this builder
         */
        public RetryConfigBuilder maxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        /**
         * Sets the initial back-off delay in milliseconds.
         *
         * @param initialBackOffMs the initial back-off in milliseconds
         * @return this builder
         */
        public RetryConfigBuilder initialBackOffMs(long initialBackOffMs) {
            this.initialBackOffMs = initialBackOffMs;
            return this;
        }

        /**
         * Sets the maximum back-off delay in milliseconds.
         *
         * @param maxBackOffMs the maximum back-off in milliseconds
         * @return this builder
         */
        public RetryConfigBuilder maxBackOffMs(long maxBackOffMs) {
            this.maxBackOffMs = maxBackOffMs;
            return this;
        }

        /**
         * Sets the multiplier applied to the back-off delay after each retry.
         *
         * @param backOffMultiplier the back-off multiplier
         * @return this builder
         */
        public RetryConfigBuilder backOffMultiplier(int backOffMultiplier) {
            this.backOffMultiplier = backOffMultiplier;
            return this;
        }

        /**
         * Sets whether retries are enabled when the server reports a rate limit.
         *
         * @param retryOnRateLimit whether rate-limit errors are retried
         * @return this builder
         */
        public RetryConfigBuilder retryOnRateLimit(boolean retryOnRateLimit) {
            this.retryOnRateLimit = retryOnRateLimit;
            return this;
        }

        /**
         * Sets the overall retry timeout in milliseconds. A value of zero means no timeout.
         *
         * @param maxRetryTimeoutMs the retry timeout in milliseconds
         * @return this builder
         */
        public RetryConfigBuilder maxRetryTimeoutMs(long maxRetryTimeoutMs) {
            this.maxRetryTimeoutMs = maxRetryTimeoutMs;
            return this;
        }

        /**
         * Builds a {@link RetryConfig}.
         *
         * @return the built retry configuration
         */
        public RetryConfig build() {
            return new RetryConfig(this);
        }
    }
}
