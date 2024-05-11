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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX;
import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.HOST_HTTPS_PREFIX;

/**
 * Parameters for client connection.
 */
@Getter
@ToString
public class ConnectParam {
    private final String host;
    private final int port;
    private final String databaseName;
    private final String uri;
    private final String token;
    private final long connectTimeoutMs;
    private final long keepAliveTimeMs;
    private final long keepAliveTimeoutMs;
    private final boolean keepAliveWithoutCalls;
    private final long rpcDeadlineMs;
    private final boolean secure;
    private final long idleTimeoutMs;
    private final String authorization;
    private final String clientKeyPath;
    private final String clientPemPath;
    private final String caPemPath;
    private final String serverPemPath;
    private final String serverName;
    private final String userName;

    protected ConnectParam(@NonNull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.token = builder.token;
        this.databaseName = builder.databaseName;
        this.uri = builder.uri;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.keepAliveTimeMs = builder.keepAliveTimeMs;
        this.keepAliveTimeoutMs = builder.keepAliveTimeoutMs;
        this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
        this.idleTimeoutMs = builder.idleTimeoutMs;
        this.rpcDeadlineMs = builder.rpcDeadlineMs;
        this.secure = builder.secure;
        this.authorization = builder.authorization;
        this.clientKeyPath = builder.clientKeyPath;
        this.clientPemPath = builder.clientPemPath;
        this.caPemPath = builder.caPemPath;
        this.serverPemPath = builder.serverPemPath;
        this.serverName = builder.serverName;
        this.userName = builder.userName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ConnectParam}
     */
    @Getter
    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private String databaseName = "default";
        private String uri;
        private String token;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = 55000;
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
        private long rpcDeadlineMs = 0; // Disabling deadline

        private String clientKeyPath;
        private String clientPemPath;
        private String caPemPath;
        private String serverPemPath;
        private String serverName;

        protected boolean secure = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        private String authorization = Base64.getEncoder().encodeToString("root:milvus".getBytes(StandardCharsets.UTF_8));

        // username/password is encoded into authorization, this member is to keep the origin username for MilvusServiceClient.connect()
        // The MilvusServiceClient.connect() is to send the client info to the server so that the server knows which client is interacting
        // If the username is unknown, send it as an empty string.
        private String userName = "";

        protected Builder() {
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
         * Sets the database name.
         *
         * @param databaseName databaseName
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the uri
         *
         * @param uri the uri of Milvus instance
         * @return <code>Builder</code>
         */
        public Builder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the token
         *
         * @param token serving as the key for identification and authentication purposes.
         * @return <code>Builder</code>
         */
        public Builder withToken(String token) {
            this.token = token;
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
         * Default is 55000 ms.
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
         * Default value is 20000 ms
         *
         * @param keepAliveTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTimeout(long keepAliveTimeout, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeoutMs = timeUnit.toMillis(keepAliveTimeout);
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
         * Deprecated from v2.3.6, this flag is auto-detected, no need to specify
         *
         * @param enable true keep-alive
         * @return <code>Builder</code>
         */
        @Deprecated
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
         * Set a deadline for how long you are willing to wait for a reply from the server.
         * With a deadline setting, the client will wait when encounter fast RPC fail caused by network fluctuations.
         * The deadline value must be larger than or equal to zero. Default value is 0, deadline is disabled.
         *
         * @param deadline deadline value
         * @param timeUnit deadline unit
         * @return <code>Builder</code>
         */
        public Builder withRpcDeadline(long deadline, @NonNull TimeUnit timeUnit) {
            this.rpcDeadlineMs = timeUnit.toMillis(deadline);
            return this;
        }

        /**
         * Sets the username and password for this connection
         * @param username current user
         * @param password password
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(String username, String password) {
            this.authorization = Base64.getEncoder().encodeToString(String.format("%s:%s", username, password).getBytes(StandardCharsets.UTF_8));
            this.userName = username;
            return this;
        }

        /**
         * Sets secure the authorization for this connection, set to True to enable TLS
         *
         * Deprecated from v2.3.6, this flag is auto-detected, no need to specify
         *
         * @param secure boolean
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withSecure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Sets the authorization for this connection
         * @param authorization the encoded authorization info that has included the encoded username and password info
         * @return <code>Builder</code>
         */
        public Builder withAuthorization(@NonNull String authorization) {
            this.authorization = authorization;
            return this;
        }

        /**
         * Set the client.key path for tls two-way authentication, only takes effect when "secure" is True.
         * @param clientKeyPath path of client.key
         * @return <code>Builder</code>
         */
        public Builder withClientKeyPath(@NonNull String clientKeyPath) {
            this.clientKeyPath = clientKeyPath;
            return this;
        }

        /**
         * Set the client.pem path for tls two-way authentication, only takes effect when "secure" is True.
         * @param clientPemPath path of client.pem
         * @return <code>Builder</code>
         */
        public Builder withClientPemPath(@NonNull String clientPemPath) {
            this.clientPemPath = clientPemPath;
            return this;
        }

        /**
         * Set the ca.pem path for tls two-way authentication, only takes effect when "secure" is True.
         * @param caPemPath path of ca.pem
         * @return <code>Builder</code>
         */
        public Builder withCaPemPath(@NonNull String caPemPath) {
            this.caPemPath = caPemPath;
            return this;
        }

        /**
         * Set the server.pem path for tls one-way authentication, only takes effect when "secure" is True.
         * @param serverPemPath path of server.pem
         * @return <code>Builder</code>
         */
        public Builder withServerPemPath(@NonNull String serverPemPath) {
            this.serverPemPath = serverPemPath;
            return this;
        }

        /**
         * Set target name override for SSL host name checking, only takes effect when "secure" is True.
         * Note: this value is passed to grpc.ssl_target_name_override
         * @param serverName override name for SSL host
         * @return <code>Builder</code>
         */
        public Builder withServerName(@NonNull String serverName) {
            this.serverName = serverName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ConnectParam} instance.
         *
         * @return {@link ConnectParam}
         */
        public ConnectParam build() throws ParamException {
            verify();

            return new ConnectParam(this);
        }

        protected void verify() throws ParamException {
            ParamUtils.CheckNullEmptyString(host, "Host name");
            if (StringUtils.isNotEmpty(uri)) {
                io.milvus.utils.URLParser result = new io.milvus.utils.URLParser(uri);
                this.secure = result.isSecure();
                this.host = result.getHostname();
                this.port = result.getPort();
                this.databaseName = StringUtils.isNotEmpty(result.getDatabase()) ? result.getDatabase() : this.databaseName;
                if (Pattern.matches(CLOUD_SERVERLESS_URI_REGEX, this.uri)) {
                    this.port = 443;
                }
            }

            if(host.startsWith(HOST_HTTPS_PREFIX)){
                this.secure = true;
            }

            if (StringUtils.isNotEmpty(token)) {
                this.authorization = Base64.getEncoder().encodeToString(String.format("%s", token).getBytes(StandardCharsets.UTF_8));
            }

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

            if (StringUtils.isNotEmpty(serverPemPath) || StringUtils.isNotEmpty(caPemPath)
                    || StringUtils.isNotEmpty(clientPemPath) || StringUtils.isNotEmpty(clientKeyPath)) {
                secure = true;
            }
        }
    }
}
