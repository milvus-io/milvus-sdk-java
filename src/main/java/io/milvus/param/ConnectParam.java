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

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * connectParam, timeUnit:ms
 */
public class ConnectParam {
    private final String host;
    private final int port;
    private final long connectTimeoutMs;
    private final long keepAliveTimeMs;
    private final long keepAliveTimeoutMs;
    private final boolean keepAliveWithoutCalls;
    private final long idleTimeoutMs;

    private ConnectParam(@Nonnull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.keepAliveTimeMs = builder.keepAliveTimeMs;
        this.keepAliveTimeoutMs = builder.keepAliveTimeoutMs;
        this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
        this.idleTimeoutMs = builder.idleTimeoutMs;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public long getKeepAliveTimeMs() {
        return keepAliveTimeMs;
    }

    public long getKeepAliveTimeoutMs() {
        return keepAliveTimeoutMs;
    }

    public boolean isKeepAliveWithoutCalls() {
        return keepAliveWithoutCalls;
    }

    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    /**
     * Builder for <code>ConnectParam</code>
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = Long.MAX_VALUE; // Disabling keepalive
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withHost(@Nonnull String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) throws IllegalArgumentException {
            if (port < 0 || port > 0xFFFF) {
                throw new IllegalArgumentException("Port is out of range!");
            }
            this.port = port;
            return this;
        }

        public Builder withConnectTimeout(long connectTimeout, @Nonnull TimeUnit timeUnit)
                throws IllegalArgumentException {
            if (connectTimeout <= 0L) {
                throw new IllegalArgumentException("Connect timeout must be positive!");
            }
            this.connectTimeoutMs = timeUnit.toMillis(connectTimeout);
            return this;
        }

        public Builder withKeepAliveTime(long keepAliveTime, @Nonnull TimeUnit timeUnit)
                throws IllegalArgumentException {
            if (keepAliveTime <= 0L) {
                throw new IllegalArgumentException("Keepalive time must be positive!");
            }
            this.keepAliveTimeMs = timeUnit.toMillis(keepAliveTime);
            return this;
        }

        public Builder withKeepAliveTimeout(long keepAliveTimeout, @Nonnull TimeUnit timeUnit)
                throws IllegalArgumentException {
            if (keepAliveTimeout <= 0L) {
                throw new IllegalArgumentException("Keepalive timeout must be positive!");
            }
            this.keepAliveTimeoutMs = timeUnit.toNanos(keepAliveTimeout);
            return this;
        }

        public Builder keepAliveWithoutCalls(boolean enable) {
            keepAliveWithoutCalls = enable;
            return this;
        }

        public Builder withIdleTimeout(long idleTimeout, @Nonnull TimeUnit timeUnit)
                throws IllegalArgumentException {
            if (idleTimeout <= 0L) {
                throw new IllegalArgumentException("Idle timeout must be positive!");
            }
            this.idleTimeoutMs = timeUnit.toMillis(idleTimeout);
            return this;
        }

        public ConnectParam build() throws ParamException {
            return new ConnectParam(this);
        }
    }
}
