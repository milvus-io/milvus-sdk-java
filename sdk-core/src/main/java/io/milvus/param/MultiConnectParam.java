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

import com.google.common.collect.Lists;
import io.milvus.exception.ParamException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.HOST_HTTPS_PREFIX;
import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.HOST_HTTP_PREFIX;

/**
 * Parameters for client connection of multi server.
 */
public class MultiConnectParam extends ConnectParam {
    private final List<ServerAddress> hosts;
    private final QueryNodeSingleSearch queryNodeSingleSearch;

    private MultiConnectParam(Builder builder) {
        super(builder);
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.hosts = builder.hosts;
        this.queryNodeSingleSearch = builder.queryNodeSingleSearch;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<ServerAddress> getHosts() {
        return hosts;
    }

    public QueryNodeSingleSearch getQueryNodeSingleSearch() {
        return queryNodeSingleSearch;
    }

    @Override
    public String toString() {
        return "MultiConnectParam{" +
                "hosts=" + hosts +
                ", queryNodeSingleSearch=" + queryNodeSingleSearch +
                ", " + super.toString() +
                '}';
    }

    /**
     * Builder for {@link MultiConnectParam}
     */
    public static class Builder extends ConnectParam.Builder {
        private List<ServerAddress> hosts;
        private QueryNodeSingleSearch queryNodeSingleSearch;

        private Builder() {
        }

        /**
         * Sets the addresses.
         *
         * @param hosts hosts serverAddresses
         * @return <code>Builder</code>
         */
        public Builder withHosts(List<ServerAddress> hosts) {
            if (hosts == null) {
                throw new IllegalArgumentException("Hosts cannot be null");
            }
            this.hosts = hosts;
            return this;
        }

        /**
         * Sets single search for query node listener.
         *
         * @param queryNodeSingleSearch query node single search for listener
         * @return <code>Builder</code>
         */
        public Builder withQueryNodeSingleSearch(QueryNodeSingleSearch queryNodeSingleSearch) {
            if (queryNodeSingleSearch == null) {
                throw new IllegalArgumentException("Query node single search cannot be null");
            }
            this.queryNodeSingleSearch = queryNodeSingleSearch;
            return this;
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
            super.withHost(host);
            return this;
        }

        /**
         * Sets the connection port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port)  {
            super.withPort(port);
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName databaseName
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            super.withDatabaseName(databaseName);
            return this;
        }

        /**
         * Sets the uri
         *
         * @param uri the uri of Milvus instance
         * @return <code>Builder</code>
         */
        public Builder withUri(String uri) {
            super.withUri(uri);
            return this;
        }

        /**
         * Sets the token
         *
         * @param token serving as the key for identification and authentication purposes.
         * @return <code>Builder</code>
         */
        public Builder withToken(String token) {
            super.withToken(token);
            return this;
        }

        /**
         * Sets the connection timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param connectTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withConnectTimeout(long connectTimeout, TimeUnit timeUnit) {
            if (timeUnit == null) {
                throw new IllegalArgumentException("TimeUnit cannot be null");
            }
            super.withConnectTimeout(connectTimeout, timeUnit);
            return this;
        }

        /**
         * Sets the keep-alive time value of client channel. The keep-alive value must be greater than zero.
         *
         * @param keepAliveTime keep-alive value
         * @param timeUnit keep-alive unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
            if (timeUnit == null) {
                throw new IllegalArgumentException("TimeUnit cannot be null");
            }
            super.withKeepAliveTime(keepAliveTime, timeUnit);
            return this;
        }

        /**
         * Sets the keep-alive timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param keepAliveTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
            if (timeUnit == null) {
                throw new IllegalArgumentException("TimeUnit cannot be null");
            }
            super.withKeepAliveTimeout(keepAliveTimeout, timeUnit);
            return this;
        }

        /**
         * Enables the keep-alive function for client channel.
         *
         * @param enable true keep-alive
         * @return <code>Builder</code>
         */
        public Builder keepAliveWithoutCalls(boolean enable) {
            super.keepAliveWithoutCalls(enable);
            return this;
        }

        /**
         * Sets the idle timeout value of client channel. The timeout value must be larger than zero.
         *
         * @param idleTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withIdleTimeout(long idleTimeout, TimeUnit timeUnit) {
            if (timeUnit == null) {
                throw new IllegalArgumentException("TimeUnit cannot be null");
            }
            super.withIdleTimeout(idleTimeout, timeUnit);
            return this;
        }

        /**
         * Set a deadline for how long you are willing to wait for a reply from the server.
         * With a deadline setting, the client will wait when encounter fast RPC fail caused by network fluctuations.
         * The deadline value must be larger than or equal to zero. Default value is 0, deadline is disabled.
         *
         * @param deadline deadline value
         * @param timeUnit deadline unit
         * @return <code>Builder</code>
         */
        public Builder withRpcDeadline(long deadline, TimeUnit timeUnit) {
            if (timeUnit == null) {
                throw new IllegalArgumentException("TimeUnit cannot be null");
            }
            super.withRpcDeadline(deadline, timeUnit);
            return this;
        }

        /**
         * Sets the username and password for this connection
         * @param username current user
         * @param password password
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(String username, String password) {
            super.withAuthorization(username, password);
            return this;
        }

        /**
         * Sets secure the authorization for this connection, set to True to enable TLS
         * @param secure boolean
         * @return <code>Builder</code>
         */
        public Builder withSecure(boolean secure) {
            super.withSecure(secure);
            return this;
        }

        /**
         * Sets the secure for this connection
         * @param authorization the authorization info that has included the encoded username and password info
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(String authorization) {
            if (authorization == null) {
                throw new IllegalArgumentException("Authorization cannot be null");
            }
            super.withAuthorization(authorization);
            return this;
        }

        /**
         * Set the client.key path for tls two-way authentication, only takes effect when "secure" is True.
         * @param clientKeyPath path of client.key
         * @return <code>Builder</code>
         */
        public Builder withClientKeyPath(String clientKeyPath) {
            if (clientKeyPath == null) {
                throw new IllegalArgumentException("Client key path cannot be null");
            }
            super.withClientKeyPath(clientKeyPath);
            return this;
        }

        /**
         * Set the client.pem path for tls two-way authentication, only takes effect when "secure" is True.
         * @param clientPemPath path of client.pem
         * @return <code>Builder</code>
         */
        public Builder withClientPemPath(String clientPemPath) {
            if (clientPemPath == null) {
                throw new IllegalArgumentException("Client pem path cannot be null");
            }
            super.withClientPemPath(clientPemPath);
            return this;
        }

        /**
         * Set the ca.pem path for tls two-way authentication, only takes effect when "secure" is True.
         * @param caPemPath path of ca.pem
         * @return <code>Builder</code>
         */
        public Builder withCaPemPath(String caPemPath) {
            if (caPemPath == null) {
                throw new IllegalArgumentException("CA pem path cannot be null");
            }
            super.withCaPemPath(caPemPath);
            return this;
        }

        /**
         * Set the server.pem path for tls two-way authentication, only takes effect when "secure" is True.
         * @param serverPemPath path of server.pem
         * @return <code>Builder</code>
         */
        public Builder withServerPemPath(String serverPemPath) {
            if (serverPemPath == null) {
                throw new IllegalArgumentException("Server pem path cannot be null");
            }
            super.withServerPemPath(serverPemPath);
            return this;
        }

        /**
         * Set target name override for SSL host name checking, only takes effect when "secure" is True.
         * Note: this value is passed to grpc.ssl_target_name_override
         * @param serverName path of server.pem
         * @return <code>Builder</code>
         */
        public Builder withServerName(String serverName) {
            if (serverName == null) {
                throw new IllegalArgumentException("Server name cannot be null");
            }
            super.withServerName(serverName);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link MultiConnectParam} instance.
         *
         * @return {@link MultiConnectParam}
         */
        public MultiConnectParam build() throws ParamException {
            super.verify();

            if (CollectionUtils.isEmpty(hosts)) {
                throw new ParamException("Server addresses is empty!");
            }

            List<ServerAddress> hostAddress = Lists.newArrayList();
            for (ServerAddress serverAddress : hosts) {
                String host = serverAddress.getHost();
                ParamUtils.CheckNullEmptyString(host, "Host name");
                if(host.startsWith(HOST_HTTPS_PREFIX)){
                    host = host.replace(HOST_HTTPS_PREFIX, "");
                    this.secure = true;
                }else if(host.startsWith(HOST_HTTP_PREFIX)){
                    host = host.replace(HOST_HTTP_PREFIX, "");
                }
                hostAddress.add(ServerAddress.newBuilder()
                        .withHost(host)
                        .withPort(serverAddress.getPort())
                        .withHealthPort(serverAddress.getHealthPort())
                        .build());

                if (serverAddress.getPort() < 0 || serverAddress.getPort() > 0xFFFF) {
                    throw new ParamException("Port is out of range!");
                }
            }
            this.withHosts(hostAddress);

            return new MultiConnectParam(this);
        }
    }
}
