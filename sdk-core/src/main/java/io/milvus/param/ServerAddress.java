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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ServerAddress {
    private final String host;
    private final int port;
    private final int healthPort;

    private ServerAddress(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        if (builder.host == null) {
            throw new IllegalArgumentException("Host cannot be null");
        }
        this.host = builder.host;
        this.port = builder.port;
        this.healthPort = builder.healthPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getHealthPort() {
        return healthPort;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private int healthPort = 9091;

        private Builder() {
        }

        /**
         * Sets the host name/address.
         *
         * @param host host name/address
         * @return <code>Builder</code>
         */
        public Builder withHost(String host) {
            if (host == null) {
                throw new IllegalArgumentException("Host cannot be null");
            }
            this.host = host;
            return this;
        }

        /**
         * Sets the connection port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the cluster health port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withHealthPort(int port) {
            this.healthPort = port;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ServerAddress} instance.
         *
         * @return {@link ServerAddress}
         */
        public ServerAddress build() throws ParamException {
            ParamUtils.CheckNullEmptyString(host, "Host name");

            if (port < 0 || port > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }

            if (healthPort < 0 || healthPort > 0xFFFF) {
                throw new ParamException("Health Port is out of range!");
            }

            return new ServerAddress(this);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ServerAddress{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ServerAddress that = (ServerAddress) o;

        return new EqualsBuilder()
                .append(port, that.port)
                .append(host, that.host)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(host)
                .append(port)
                .toHashCode();
    }
}
