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

package io.milvus.param;

import io.milvus.exception.ParamException;

/**
 * Parameters for retry on failure.
 */
public class RetryParam {
    private int maxRetryTimes;
    private long initialBackOffMs;
    private long maxBackOffMs;
    private int backOffMultiplier;
    private boolean retryOnRateLimit;

    protected RetryParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.maxRetryTimes = builder.maxRetryTimes;
        this.initialBackOffMs = builder.initialBackOffMs;
        this.maxBackOffMs = builder.maxBackOffMs;
        this.backOffMultiplier = builder.backOffMultiplier;
        this.retryOnRateLimit = builder.retryOnRateLimit;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getInitialBackOffMs() {
        return initialBackOffMs;
    }

    public void setInitialBackOffMs(long initialBackOffMs) {
        this.initialBackOffMs = initialBackOffMs;
    }

    public long getMaxBackOffMs() {
        return maxBackOffMs;
    }

    public void setMaxBackOffMs(long maxBackOffMs) {
        this.maxBackOffMs = maxBackOffMs;
    }

    public int getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public void setBackOffMultiplier(int backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    public boolean isRetryOnRateLimit() {
        return retryOnRateLimit;
    }

    public void setRetryOnRateLimit(boolean retryOnRateLimit) {
        this.retryOnRateLimit = retryOnRateLimit;
    }

    @Override
    public String toString() {
        return "RetryParam{" +
                "maxRetryTimes=" + maxRetryTimes +
                ", initialBackOffMs=" + initialBackOffMs +
                ", maxBackOffMs=" + maxBackOffMs +
                ", backOffMultiplier=" + backOffMultiplier +
                ", retryOnRateLimit=" + retryOnRateLimit +
                '}';
    }

    /**
     * Builder for {@link RetryParam}
     */
    public static class Builder {
        private int maxRetryTimes = 75;
        private long initialBackOffMs = 10;
        private long maxBackOffMs = 3000;
        private int backOffMultiplier = 3;
        private boolean retryOnRateLimit = true;

        protected Builder() {
        }

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

        /**
         * Sets the max retry times on failure.Default value is 75.
         *
         * @param maxRetryTimes the maxinum times to retry
         * @return <code>Builder</code>
         */
        public Builder withMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        /**
         * Sets the first time interval between two retries, units: millisecond. Default value is 10ms.
         *
         * @param initialBackOffMs time interval value
         * @return <code>Builder</code>
         */
        public Builder withInitialBackOffMs(long initialBackOffMs) {
            this.initialBackOffMs = initialBackOffMs;
            return this;
        }

        /**
         * Sets the maximum time interval between two retries, units: millisecond. Default value is 3000ms.
         *
         * @param maxBackOffMs time interval value
         * @return <code>Builder</code>
         */
        public Builder withMaxBackOffMs(long maxBackOffMs) {
            this.maxBackOffMs = maxBackOffMs;
            return this;
        }

        /**
         * Sets multiplier to increase time interval after each retry. Default value is 3.
         *
         * @param backOffMultiplier the multiplier to increase time interval after each retry
         * @return <code>Builder</code>
         */
        public Builder withBackOffMultiplier(int backOffMultiplier) {
            this.backOffMultiplier = backOffMultiplier;
            return this;
        }

        /**
         * Sets whether to retry when the returned error is rate limit.Default value is true.
         *
         * @param retryOnRateLimit whether to retry when the returned error is rate limit
         * @return <code>Builder</code>
         */
        public Builder withRetryOnRateLimit(boolean retryOnRateLimit) {
            this.retryOnRateLimit = retryOnRateLimit;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link RetryParam} instance.
         *
         * @return {@link RetryParam}
         */
        public RetryParam build() throws ParamException {
            if (maxRetryTimes <= 0L) {
                throw new ParamException("Max retry time value must be positive!");
            }

            if (initialBackOffMs <= 0L) {
                throw new ParamException("The initial time interval must be positive!");
            }

            if (maxBackOffMs <= 0L) {
                throw new ParamException("The max time interval must be positive!");
            }

            if (backOffMultiplier <= 0L) {
                throw new ParamException("The multiplier to increase time interval must be positive!");
            }

            return new RetryParam(this);
        }
    }
}
