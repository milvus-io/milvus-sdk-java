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

import lombok.NonNull;
import java.util.concurrent.TimeUnit;

/**
 * Parameters for client connection.
 */
public class ConnectParam {
    private final String host;
    private final int port;
    private final long connectTimeoutMs;
    private final long keepAliveTimeMs;
    private final long keepAliveTimeoutMs;
    private final boolean keepAliveWithoutCalls;
    private final long idleTimeoutMs;

    private ConnectParam(@NonNull Builder builder) {
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

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>ConnectParam</code>
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = Long.MAX_VALUE; // Disabling keep alive
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);

        private Builder() {
        }

        /**
         * Set host name/address.
         *
         * @param host host name/address
         * @return <code>Builder</code>
         */
        public Builder withHost(@NonNull String host) {
            this.host = host;
            return this;
        }

        /**
         * Set connection port. Port value must be larger than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port)  {
            this.port = port;
            return this;
        }

        /**
         * Set connect time out value of client channel. The time out value must be larger than zero.
         *
         * @param connectTimeout time out value
         * @param timeUnit time out unit
         * @return <code>Builder</code>
         */
        public Builder withConnectTimeout(long connectTimeout, @NonNull TimeUnit timeUnit) {
            this.connectTimeoutMs = timeUnit.toMillis(connectTimeout);
            return this;
        }

        /**
         * Set keep alive time value of client channel. The time out value must be larger than zero.
         *
         * @param keepAliveTime time out value
         * @param timeUnit time out unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTime(long keepAliveTime, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeMs = timeUnit.toMillis(keepAliveTime);
            return this;
        }

        /**
         * Set keep alive time out value of client channel. The time out value must be larger than zero.
         *
         * @param keepAliveTimeout time out value
         * @param timeUnit time out unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTimeout(long keepAliveTimeout, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeoutMs = timeUnit.toNanos(keepAliveTimeout);
            return this;
        }

        /**
         * Set client channel keep alive.
         *
         * @param enable true keep alive
         * @return <code>Builder</code>
         */
        public Builder keepAliveWithoutCalls(boolean enable) {
            keepAliveWithoutCalls = enable;
            return this;
        }

        /**
         * Set idle time out value of client channel. The time out value must be larger than zero.
         *
         * @param idleTimeout time out value
         * @param timeUnit time out unit
         * @return <code>Builder</code>
         */
        public Builder withIdleTimeout(long idleTimeout, @NonNull TimeUnit timeUnit) {
            this.idleTimeoutMs = timeUnit.toMillis(idleTimeout);
            return this;
        }

        /**
         * Verify parameters and create a new <code>ConnectParam</code> instance.
         *
         * @return <code>ShowCollectionsParam</code>
         */
        public ConnectParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(host, "Host name");

            if (port < 0 || port > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }

            if (keepAliveTimeMs <= 0L) {
                throw new ParamException("Keep alive time must be positive!");
            }

            if (connectTimeoutMs <= 0L) {
                throw new ParamException("Connect timeout must be positive!");
            }

            if (keepAliveTimeoutMs <= 0L) {
                throw new ParamException("Keep alive timeout must be positive!");
            }

            if (idleTimeoutMs <= 0L) {
                throw new ParamException("Idle timeout must be positive!");
            }

            return new ConnectParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>ConnectParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ConnectParam{" +
                "host='" + host + '\'' +
                ", port='" + port +
                '}';
    }
}
