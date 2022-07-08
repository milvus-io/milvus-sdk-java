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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
    private final boolean secure;
    private final long idleTimeoutMs;
    private final String authorization;

    private ConnectParam(@NonNull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.keepAliveTimeMs = builder.keepAliveTimeMs;
        this.keepAliveTimeoutMs = builder.keepAliveTimeoutMs;
        this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
        this.idleTimeoutMs = builder.idleTimeoutMs;
        this.secure = builder.secure;
        this.authorization = builder.authorization;
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

    public boolean isSecure() {
        return secure;
    }

    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public String getAuthorization() {
        return authorization;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ConnectParam}
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = Long.MAX_VALUE; // Disabling keep alive
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
        private boolean secure = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        private String authorization = "";

        private Builder() {
        }

        /**
         * Sets the host name/address.
         *
         * @param host host name/address
         * @return <code>Builder</code>
         */
        public Builder withHost(@NonNull String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the connection port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port)  {
            this.port = port;
            return this;
        }

        /**
         * Sets the connection timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param connectTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withConnectTimeout(long connectTimeout, @NonNull TimeUnit timeUnit) {
            this.connectTimeoutMs = timeUnit.toMillis(connectTimeout);
            return this;
        }

        /**
         * Sets the keep-alive time value of client channel. The keep-alive value must be greater than zero.
         *
         * @param keepAliveTime keep-alive value
         * @param timeUnit keep-alive unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTime(long keepAliveTime, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeMs = timeUnit.toMillis(keepAliveTime);
            return this;
        }

        /**
         * Sets the keep-alive timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param keepAliveTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTimeout(long keepAliveTimeout, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeoutMs = timeUnit.toNanos(keepAliveTimeout);
            return this;
        }

        /**
         * Enables the keep-alive function for client channel.
         *
         * @param enable true keep-alive
         * @return <code>Builder</code>
         */
        public Builder keepAliveWithoutCalls(boolean enable) {
            keepAliveWithoutCalls = enable;
            return this;
        }

        /**
         * Enables the secure for client channel.
         *
         * @param enable true keep-alive
         * @return <code>Builder</code>
         */
        public Builder secure(boolean enable) {
            secure = enable;
            return this;
        }

        /**
         * Sets the idle timeout value of client channel. The timeout value must be larger than zero.
         *
         * @param idleTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withIdleTimeout(long idleTimeout, @NonNull TimeUnit timeUnit) {
            this.idleTimeoutMs = timeUnit.toMillis(idleTimeout);
            return this;
        }

        /**
         * Sets the username and password for this connection
         * @param username current user
         * @param password password
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(@NonNull String username, @NonNull String password) {
            this.authorization = Base64.getEncoder().encodeToString(String.format("%s:%s", username, password).getBytes(StandardCharsets.UTF_8));
            return this;
        }

        /**
         * Sets secure the authorization for this connection
         * @param secure boolean
         * @return <code>Builder</code>
         */
        public Builder withSecure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Sets the secure for this connection
         * @param authorization the authorization info that has included the encoded username and password info
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(@NonNull String authorization) {
            this.authorization = authorization;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ConnectParam} instance.
         *
         * @return {@link ConnectParam}
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
     * Constructs a <code>String</code> by {@link ConnectParam} instance.
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
