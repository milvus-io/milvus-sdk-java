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

package io.milvus.telemetry;

/** Configuration for client telemetry and server-pushed commands. */
public final class TelemetryConfig {
    private volatile boolean enabled;
    private volatile long heartbeatIntervalMs;
    private volatile double samplingRate;
    private final int errorMaxCount;
    private final String clientId;

    private TelemetryConfig(Builder builder) {
        if (builder.heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
        }
        this.enabled = builder.enabled;
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
        this.samplingRate = clampSamplingRate(builder.samplingRate);
        this.errorMaxCount = builder.errorMaxCount > 0 ? builder.errorMaxCount : 100;
        this.clientId = builder.clientId == null ? "" : builder.clientId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TelemetryConfig defaults() {
        return builder().build();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public int getErrorMaxCount() {
        return errorMaxCount;
    }

    public String getClientId() {
        return clientId;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeat_interval_ms must be positive");
        }
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    void setSamplingRate(double samplingRate) {
        this.samplingRate = clampSamplingRate(samplingRate);
    }

    private static double clampSamplingRate(double samplingRate) {
        return Math.max(0.0, Math.min(1.0, samplingRate));
    }

    public static final class Builder {
        private boolean enabled = true;
        private long heartbeatIntervalMs = 30_000;
        private double samplingRate = 1.0;
        private int errorMaxCount = 100;
        private String clientId = "";

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        public Builder samplingRate(double samplingRate) {
            this.samplingRate = samplingRate;
            return this;
        }

        public Builder errorMaxCount(int errorMaxCount) {
            this.errorMaxCount = errorMaxCount;
            return this;
        }

        /** Pins the telemetry client ID across process restarts. */
        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public TelemetryConfig build() {
            return new TelemetryConfig(this);
        }
    }
}
