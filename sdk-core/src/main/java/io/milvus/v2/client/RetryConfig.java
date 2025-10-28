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

public class RetryConfig {
    private int maxRetryTimes = 75;
    private long initialBackOffMs = 10;
    private long maxBackOffMs = 3000;
    private int backOffMultiplier = 3;
    private boolean retryOnRateLimit = true;
    private long maxRetryTimeoutMs = 0;

    // Constructor for builder pattern
    private RetryConfig(RetryConfigBuilder builder) {
        this.maxRetryTimes = builder.maxRetryTimes;
        this.initialBackOffMs = builder.initialBackOffMs;
        this.maxBackOffMs = builder.maxBackOffMs;
        this.backOffMultiplier = builder.backOffMultiplier;
        this.retryOnRateLimit = builder.retryOnRateLimit;
        this.maxRetryTimeoutMs = builder.maxRetryTimeoutMs;
    }

    public static RetryConfigBuilder builder() {
        return new RetryConfigBuilder();
    }

    // Getters
    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public long getInitialBackOffMs() {
        return initialBackOffMs;
    }

    public long getMaxBackOffMs() {
        return maxBackOffMs;
    }

    public int getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public boolean isRetryOnRateLimit() {
        return retryOnRateLimit;
    }

    public long getMaxRetryTimeoutMs() {
        return maxRetryTimeoutMs;
    }

    // Setters
    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public void setInitialBackOffMs(long initialBackOffMs) {
        this.initialBackOffMs = initialBackOffMs;
    }

    public void setMaxBackOffMs(long maxBackOffMs) {
        this.maxBackOffMs = maxBackOffMs;
    }

    public void setBackOffMultiplier(int backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    public void setRetryOnRateLimit(boolean retryOnRateLimit) {
        this.retryOnRateLimit = retryOnRateLimit;
    }

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

        public RetryConfigBuilder maxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public RetryConfigBuilder initialBackOffMs(long initialBackOffMs) {
            this.initialBackOffMs = initialBackOffMs;
            return this;
        }

        public RetryConfigBuilder maxBackOffMs(long maxBackOffMs) {
            this.maxBackOffMs = maxBackOffMs;
            return this;
        }

        public RetryConfigBuilder backOffMultiplier(int backOffMultiplier) {
            this.backOffMultiplier = backOffMultiplier;
            return this;
        }

        public RetryConfigBuilder retryOnRateLimit(boolean retryOnRateLimit) {
            this.retryOnRateLimit = retryOnRateLimit;
            return this;
        }

        public RetryConfigBuilder maxRetryTimeoutMs(long maxRetryTimeoutMs) {
            this.maxRetryTimeoutMs = maxRetryTimeoutMs;
            return this;
        }

        public RetryConfig build() {
            return new RetryConfig(this);
        }
    }
}
