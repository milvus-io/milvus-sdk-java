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

import io.milvus.common.utils.URLParser;
import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.telemetry.TelemetryConfig;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX;
import static io.milvus.common.utils.RedactCredential.redactCredential;
import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;

/**
 * Configuration for connecting to a Milvus server.
 * <p>
 * Use {@link #builder()} to create a configuration, then pass it to the
 * {@code MilvusClientV2} constructor. The builder validates required fields such as {@code uri}.
 */
public class ConnectConfig {
    private String uri;
    private String token;
    private String username;
    private String password;
    private String dbName;
    private long connectTimeoutMs = 10000;
    private long keepAliveTimeMs = 10000;
    private long keepAliveTimeoutMs = 5000;
    private boolean keepAliveWithoutCalls = true;
    private long rpcDeadlineMs = 0; // Disabling deadline

    private String clientKeyPath;
    private String clientPemPath;
    private String caPemPath;
    private String serverPemPath;
    private String serverName;
    private String proxyAddress;
    private Boolean secure = false;
    private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
    private boolean enablePrecheck = false;  // default value is false
    private Map<String, String> option = new HashMap<>();

    private SSLContext sslContext;
    // clientRequestId maintains a map for different threads, each thread can assign a specific id.
    // the specific id is passed to the server, from the access log we can know which client calls the interface
    private ThreadLocal<String> clientRequestId;
    private TelemetryConfig telemetryConfig = TelemetryConfig.defaults();
    private String telemetryClientId = "";
    private ClientTelemetryManager.RuntimeState telemetryRuntimeState;
    private boolean deferTelemetryStart;

    // Constructor for builder
    private ConnectConfig(ConnectConfigBuilder builder) {
        if (builder.uri == null) {
            throw new NullPointerException("uri is marked non-null but is null");
        }
        this.uri = builder.uri;
        this.token = builder.token;
        this.username = builder.username;
        this.password = builder.password;
        this.dbName = builder.dbName;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.keepAliveTimeMs = builder.keepAliveTimeMs;
        this.keepAliveTimeoutMs = builder.keepAliveTimeoutMs;
        this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
        this.rpcDeadlineMs = builder.rpcDeadlineMs;
        this.clientKeyPath = builder.clientKeyPath;
        this.clientPemPath = builder.clientPemPath;
        this.caPemPath = builder.caPemPath;
        this.serverPemPath = builder.serverPemPath;
        this.serverName = builder.serverName;
        this.proxyAddress = builder.proxyAddress;
        this.secure = builder.secure;
        this.idleTimeoutMs = builder.idleTimeoutMs;
        this.sslContext = builder.sslContext;
        this.clientRequestId = builder.clientRequestId;
        this.telemetryConfig = builder.telemetryConfig;
        this.telemetryClientId = builder.telemetryClientId;
        this.telemetryRuntimeState = builder.telemetryRuntimeState;
        this.deferTelemetryStart = builder.deferTelemetryStart;
        this.enablePrecheck = builder.enablePrecheck;
        this.option = builder.option;
    }

    /**
    * Returns a builder for {@link ConnectConfig}.
    *
    * @return a configuration builder
    */
    public static ConnectConfigBuilder builder() {
        return new ConnectConfigBuilder();
    }

    // Getters
    /**
    * Returns the connection URI of the Milvus server.
    *
    * @return the connection URI of the Milvus server
    */
    public String getUri() {
        return uri;
    }

    /**
    * Returns the authentication token.
    *
    * @return the authentication token
    */
    public String getToken() {
        return token;
    }

    /**
    * Returns the username used for authentication.
    *
    * @return the username used for authentication
    */
    public String getUsername() {
        return username;
    }

    /**
    * Returns the password used for authentication.
    *
    * @return the password used for authentication
    */
    public String getPassword() {
        return password;
    }

    /**
    * Returns the connection timeout in milliseconds.
    *
    * @return the connection timeout in milliseconds
    */
    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    /**
    * Returns the keep-alive time in milliseconds.
    *
    * @return the keep-alive time in milliseconds
    */
    public long getKeepAliveTimeMs() {
        return keepAliveTimeMs;
    }

    /**
    * Returns the keep-alive timeout in milliseconds.
    *
    * @return the keep-alive timeout in milliseconds
    */
    public long getKeepAliveTimeoutMs() {
        return keepAliveTimeoutMs;
    }

    /**
    * Returns whether the channel is kept alive when there are no active calls.
    *
    * @return true if keep-alive is enabled for idle channels
    */
    public boolean isKeepAliveWithoutCalls() {
        return keepAliveWithoutCalls;
    }

    /**
    * Returns the RPC deadline in milliseconds.
    *
    * @return the RPC deadline in milliseconds
    */
    public long getRpcDeadlineMs() {
        return rpcDeadlineMs;
    }

    /**
    * Returns the path to the client key file.
    *
    * @return the path to the client key file
    */
    public String getClientKeyPath() {
        return clientKeyPath;
    }

    /**
    * Returns the path to the client certificate (PEM) file.
    *
    * @return the path to the client certificate (PEM) file
    */
    public String getClientPemPath() {
        return clientPemPath;
    }

    /**
    * Returns the path to the CA certificate (PEM) file.
    *
    * @return the path to the CA certificate (PEM) file
    */
    public String getCaPemPath() {
        return caPemPath;
    }

    /**
    * Returns the path to the server certificate (PEM) file.
    *
    * @return the path to the server certificate (PEM) file
    */
    public String getServerPemPath() {
        return serverPemPath;
    }

    /**
    * Returns the server name used for TLS verification.
    *
    * @return the server name used for TLS verification
    */
    public String getServerName() {
        return serverName;
    }

    /**
    * Returns whether a secure (TLS) connection is used.
    *
    * @return whether a secure (TLS) connection is used
    */
    public Boolean getSecure() {
        return secure;
    }

    /**
    * Returns the idle timeout in milliseconds.
    *
    * @return the idle timeout in milliseconds
    */
    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    /**
    * Returns the SSL context used for TLS connections.
    *
    * @return the SSL context used for TLS connections
    */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
    * Returns the thread-local client request ID.
    *
    * @return the thread-local client request ID
    */
    public ThreadLocal<String> getClientRequestId() {
        return clientRequestId;
    }

    /**
    * Returns the telemetry configuration.
    *
    * @return the telemetry configuration
    */
    public TelemetryConfig getTelemetryConfig() {
        return telemetryConfig;
    }

    /**
    * Returns the telemetry client ID.
    *
    * @return the telemetry client ID
    */
    public String getTelemetryClientId() {
        return telemetryClientId;
    }

    /**
    * Takes and clears the telemetry runtime state, restoring it once.
    *
    * @return the stored telemetry runtime state, or null if none is stored
    */
    public synchronized ClientTelemetryManager.RuntimeState takeTelemetryRuntimeState() {
        ClientTelemetryManager.RuntimeState state = telemetryRuntimeState;
        telemetryRuntimeState = null;
        return state;
    }

    /**
    * Returns whether starting the telemetry worker is deferred.
    *
    * @return true if telemetry start is deferred
    */
    public boolean isDeferTelemetryStart() {
        return deferTelemetryStart;
    }

    /**
    * Returns the proxy address.
    *
    * @return the proxy address
    */
    public String getProxyAddress() {
        return proxyAddress;
    }

    /**
    * Returns whether host, port and certificates are prechecked before connecting.
    *
    * @return true if precheck is enabled
    */
    public boolean isEnablePrecheck() {
        return enablePrecheck;
    }

    /**
    * Returns additional connection options.
    *
    * @return additional connection options
    */
    public Map<String, String> getOption() {
        return option;
    }
    // Setters
    /**
    * Sets the connection URI of the Milvus server.
    *
    * @param uri the connection URI of the Milvus server
    */
    public void setUri(String uri) {
        if (uri == null) {
            throw new NullPointerException("uri is marked non-null but is null");
        }
        this.uri = uri;
    }

    /**
    * Sets the authentication token.
    *
    * @param token the authentication token
    */
    public void setToken(String token) {
        this.token = token;
    }

    /**
    * Sets the username used for authentication.
    *
    * @param username the username used for authentication
    */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
    * Sets the password used for authentication.
    *
    * @param password the password used for authentication
    */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
    * Sets the database name.
    *
    * @param dbName the database name
    */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
    * Sets the connection timeout in milliseconds.
    *
    * @param connectTimeoutMs the connection timeout in milliseconds
    */
    public void setConnectTimeoutMs(long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    /**
    * Sets the keep-alive time in milliseconds.
    *
    * @param keepAliveTimeMs the keep-alive time in milliseconds
    */
    public void setKeepAliveTimeMs(long keepAliveTimeMs) {
        this.keepAliveTimeMs = keepAliveTimeMs;
    }

    /**
    * Sets the keep-alive timeout in milliseconds.
    *
    * @param keepAliveTimeoutMs the keep-alive timeout in milliseconds
    */
    public void setKeepAliveTimeoutMs(long keepAliveTimeoutMs) {
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
    }

    /**
    * Sets whether to keep the channel alive when there are no active calls.
    *
    * @param keepAliveWithoutCalls whether to keep the channel alive when there are no active calls
    */
    public void setKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    /**
    * Sets the RPC deadline in milliseconds.
    *
    * @param rpcDeadlineMs the RPC deadline in milliseconds
    */
    public void setRpcDeadlineMs(long rpcDeadlineMs) {
        this.rpcDeadlineMs = rpcDeadlineMs;
    }

    /**
    * Sets the path to the client key file.
    *
    * @param clientKeyPath the path to the client key file
    */
    public void setClientKeyPath(String clientKeyPath) {
        this.clientKeyPath = clientKeyPath;
    }

    /**
    * Sets the path to the client certificate (PEM) file.
    *
    * @param clientPemPath the path to the client certificate (PEM) file
    */
    public void setClientPemPath(String clientPemPath) {
        this.clientPemPath = clientPemPath;
    }

    /**
    * Sets the path to the CA certificate (PEM) file.
    *
    * @param caPemPath the path to the CA certificate (PEM) file
    */
    public void setCaPemPath(String caPemPath) {
        this.caPemPath = caPemPath;
    }

    /**
    * Sets the path to the server certificate (PEM) file.
    *
    * @param serverPemPath the path to the server certificate (PEM) file
    */
    public void setServerPemPath(String serverPemPath) {
        this.serverPemPath = serverPemPath;
    }

    /**
    * Sets the server name used for TLS verification.
    *
    * @param serverName the server name used for TLS verification
    */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
    * Sets the proxy address.
    *
    * @param proxyAddress the proxy address
    */
    public void setProxyAddress(String proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    /**
    * Sets whether to use a secure (TLS) connection.
    *
    * @param secure whether to use a secure (TLS) connection
    */
    public void setSecure(Boolean secure) {
        this.secure = secure;
    }

    /**
    * Sets whether to precheck host, port and certificates before connecting.
    *
    * @param enablePrecheck whether to precheck host, port and certificates before connecting
    */
    public void setEnablePrecheck(boolean enablePrecheck) {
        this.enablePrecheck = enablePrecheck;
    }

    /**
    * Sets additional connection options.
    *
    * @param option additional connection options
    */
    public void setOption(Map<String, String> option) {
        this.option = option;
    }

    /**
    * Sets the idle timeout in milliseconds.
    *
    * @param idleTimeoutMs the idle timeout in milliseconds
    */
    public void setIdleTimeoutMs(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    /**
    * Sets the SSL context used for TLS connections.
    *
    * @param sslContext the SSL context used for TLS connections
    */
    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
    * Sets the thread-local client request ID.
    *
    * @param clientRequestId the thread-local client request ID
    */
    public void setClientRequestId(ThreadLocal<String> clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    /**
    * Sets the telemetry configuration.
    *
    * @param telemetryConfig the telemetry configuration
    */
    public void setTelemetryConfig(TelemetryConfig telemetryConfig) {
        this.telemetryConfig = telemetryConfig == null ? TelemetryConfig.defaults() : telemetryConfig;
    }

    /**
    * Sets the telemetry client ID.
    *
    * @param telemetryClientId the telemetry client ID
    */
    public void setTelemetryClientId(String telemetryClientId) {
        this.telemetryClientId = telemetryClientId == null ? "" : telemetryClientId;
    }

    /**
    * Sets the telemetry runtime state to be restored on the next client construction.
    *
    * @param telemetryRuntimeState the telemetry runtime state
    */
    public synchronized void setTelemetryRuntimeState(ClientTelemetryManager.RuntimeState telemetryRuntimeState) {
        this.telemetryRuntimeState = telemetryRuntimeState;
    }

    /**
    * Returns the host parsed from the connection URI.
    *
    * @return the host
    */
    public String getHost() {
        URLParser urlParser = new URLParser(this.uri);
        return urlParser.getHostname();
    }

    /**
    * Returns the port parsed from the connection URI.
    * For serverless endpoints the port is 443.
    *
    * @return the port
    */
    public int getPort() {
        URLParser urlParser = new URLParser(this.uri);
        int port = urlParser.getPort();
        if (Pattern.matches(CLOUD_SERVERLESS_URI_REGEX, this.uri)) {
            port = 443;
        }
        return port;
    }

    /**
    * Returns the authorization value: the token if set, otherwise {@code username:password}.
    *
    * @return the authorization value, or null if no credentials are configured
    */
    public String getAuthorization() {
        if (token != null) {
            return token;
        } else if (username != null && password != null) {
            return username + ":" + password;
        }
        return null;
    }

    /**
    * Returns the effective database name, preferring the database in the connection URI
    * over the explicitly configured database name.
    *
    * @return the database name
    */
    public String getDbName() {
        URLParser urlParser = new URLParser(this.uri);
        return StringUtils.isNotEmpty(urlParser.getDatabase()) ? urlParser.getDatabase() : this.dbName;
    }

    /**
    * Returns whether the connection is secure, which is true for {@code https} URIs
    * or when a secure connection is explicitly configured.
    *
    * @return true if the connection is secure
    */
    public Boolean isSecure() {
        if (uri.startsWith("https")) {
            return true;
        }
        return secure;
    }

    @Override
    public String toString() {
        return "ConnectConfig{" +
                "uri='" + redactUriUserInfo(uri) + '\'' +
                ", token='" + redactCredential(token) + '\'' +
                ", username='" + username + '\'' +
                ", password='" + redactCredential(password) + '\'' +
                ", dbName='" + dbName + '\'' +
                ", connectTimeoutMs=" + connectTimeoutMs +
                ", keepAliveTimeMs=" + keepAliveTimeMs +
                ", keepAliveTimeoutMs=" + keepAliveTimeoutMs +
                ", keepAliveWithoutCalls=" + keepAliveWithoutCalls +
                ", rpcDeadlineMs=" + rpcDeadlineMs +
                ", clientKeyPath='" + clientKeyPath + '\'' +
                ", clientPemPath='" + clientPemPath + '\'' +
                ", caPemPath='" + caPemPath + '\'' +
                ", serverPemPath='" + serverPemPath + '\'' +
                ", serverName='" + serverName + '\'' +
                ", proxyAddress='" + proxyAddress + '\'' +
                ", secure=" + secure +
                ", enablePrecheck=" + enablePrecheck +
                ", idleTimeoutMs=" + idleTimeoutMs +
                ", sslContext=" + sslContext +
                ", clientRequestId=" + clientRequestId +
                '}';
    }

    public static class ConnectConfigBuilder {
        private String uri;
        private String token;
        private String username;
        private String password;
        private String dbName;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = 10000;
        private long keepAliveTimeoutMs = 5000;
        private boolean keepAliveWithoutCalls = true;
        private long rpcDeadlineMs = 0;
        private String clientKeyPath;
        private String clientPemPath;
        private String caPemPath;
        private String serverPemPath;
        private String serverName;
        private String proxyAddress;
        private Boolean secure = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        private SSLContext sslContext;
        private ThreadLocal<String> clientRequestId;
        private TelemetryConfig telemetryConfig = TelemetryConfig.defaults();
        private String telemetryClientId = "";
        private ClientTelemetryManager.RuntimeState telemetryRuntimeState;
        private boolean deferTelemetryStart;
        private boolean enablePrecheck = false;
        private Map<String, String> option = new HashMap<>();

        /**
        * Sets the connection URI of the Milvus server.
        *
        * @param uri the connection URI of the Milvus server
        * @return this builder
        */
        public ConnectConfigBuilder uri(String uri) {
            if (uri == null) {
                throw new NullPointerException("uri is marked non-null but is null");
            }
            this.uri = uri;
            return this;
        }

        /**
        * Sets the authentication token.
        *
        * @param token the authentication token
        * @return this builder
        */
        public ConnectConfigBuilder token(String token) {
            this.token = token;
            return this;
        }

        /**
        * Sets the username used for authentication.
        *
        * @param username the username used for authentication
        * @return this builder
        */
        public ConnectConfigBuilder username(String username) {
            if (username == null || username.trim().isEmpty()) {
                throw new IllegalArgumentException("Username cannot be null or blank");
            }
            this.username = username;
            return this;
        }

        /**
        * Sets the password used for authentication.
        *
        * @param password the password used for authentication
        * @return this builder
        */
        public ConnectConfigBuilder password(String password) {
            this.password = password;
            return this;
        }

        /**
        * Sets the database name.
        *
        * @param dbName the database name
        * @return this builder
        */
        public ConnectConfigBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
        * Sets the connection timeout in milliseconds.
        *
        * @param connectTimeoutMs the connection timeout in milliseconds
        * @return this builder
        */
        public ConnectConfigBuilder connectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        /**
        * Sets the keep-alive time in milliseconds.
        *
        * @param keepAliveTimeMs the keep-alive time in milliseconds
        * @return this builder
        */
        public ConnectConfigBuilder keepAliveTimeMs(long keepAliveTimeMs) {
            this.keepAliveTimeMs = keepAliveTimeMs;
            return this;
        }

        /**
        * Sets the keep-alive timeout in milliseconds.
        *
        * @param keepAliveTimeoutMs the keep-alive timeout in milliseconds
        * @return this builder
        */
        public ConnectConfigBuilder keepAliveTimeoutMs(long keepAliveTimeoutMs) {
            this.keepAliveTimeoutMs = keepAliveTimeoutMs;
            return this;
        }

        /**
        * Sets whether to keep the channel alive when there are no active calls.
        *
        * @param keepAliveWithoutCalls whether to keep the channel alive when there are no active calls
        * @return this builder
        */
        public ConnectConfigBuilder keepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;
            return this;
        }

        /**
        * Sets the RPC deadline in milliseconds.
        *
        * @param rpcDeadlineMs the RPC deadline in milliseconds
        * @return this builder
        */
        public ConnectConfigBuilder rpcDeadlineMs(long rpcDeadlineMs) {
            this.rpcDeadlineMs = rpcDeadlineMs;
            return this;
        }

        /**
        * Sets the path to the client key file.
        *
        * @param clientKeyPath the path to the client key file
        * @return this builder
        */
        public ConnectConfigBuilder clientKeyPath(String clientKeyPath) {
            this.clientKeyPath = clientKeyPath;
            return this;
        }

        /**
        * Sets the path to the client certificate (PEM) file.
        *
        * @param clientPemPath the path to the client certificate (PEM) file
        * @return this builder
        */
        public ConnectConfigBuilder clientPemPath(String clientPemPath) {
            this.clientPemPath = clientPemPath;
            return this;
        }

        /**
        * Sets the path to the CA certificate (PEM) file.
        *
        * @param caPemPath the path to the CA certificate (PEM) file
        * @return this builder
        */
        public ConnectConfigBuilder caPemPath(String caPemPath) {
            this.caPemPath = caPemPath;
            return this;
        }

        /**
        * Sets the path to the server certificate (PEM) file.
        *
        * @param serverPemPath the path to the server certificate (PEM) file
        * @return this builder
        */
        public ConnectConfigBuilder serverPemPath(String serverPemPath) {
            this.serverPemPath = serverPemPath;
            return this;
        }

        /**
        * Sets the server name used for TLS verification.
        *
        * @param serverName the server name used for TLS verification
        * @return this builder
        */
        public ConnectConfigBuilder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        /**
        * Sets the proxy address.
        *
        * @param proxyAddress the proxy address
        * @return this builder
        */
        public ConnectConfigBuilder proxyAddress(String proxyAddress) {
            this.proxyAddress = proxyAddress;
            return this;
        }

        /**
        * Sets whether to use a secure (TLS) connection.
        *
        * @param secure whether to use a secure (TLS) connection
        * @return this builder
        */
        public ConnectConfigBuilder secure(Boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
        * Sets whether to precheck host, port and certificates before connecting.
        *
        * @param enablePrecheck whether to precheck host, port and certificates before connecting
        * @return this builder
        */
        public ConnectConfigBuilder enablePrecheck(boolean enablePrecheck) {
            this.enablePrecheck = enablePrecheck;
            return this;
        }

        /**
        * Sets the idle timeout in milliseconds.
        *
        * @param idleTimeoutMs the idle timeout in milliseconds
        * @return this builder
        */
        public ConnectConfigBuilder idleTimeoutMs(long idleTimeoutMs) {
            this.idleTimeoutMs = idleTimeoutMs;
            return this;
        }

        /**
        * Sets the SSL context used for TLS connections.
        *
        * @param sslContext the SSL context used for TLS connections
        * @return this builder
        */
        public ConnectConfigBuilder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
        * Sets the thread-local client request ID.
        *
        * @param clientRequestId the thread-local client request ID
        * @return this builder
        */
        public ConnectConfigBuilder clientRequestId(ThreadLocal<String> clientRequestId) {
            this.clientRequestId = clientRequestId;
            return this;
        }

        /**
        * Sets the telemetry configuration.
        *
        * @param telemetryConfig the telemetry configuration
        * @return this builder
        */
        public ConnectConfigBuilder telemetryConfig(TelemetryConfig telemetryConfig) {
            this.telemetryConfig = telemetryConfig == null ? TelemetryConfig.defaults() : telemetryConfig;
            return this;
        }

        /**
        * Sets the telemetry client ID.
        *
        * @param telemetryClientId the telemetry client ID
        * @return this builder
        */
        public ConnectConfigBuilder telemetryClientId(String telemetryClientId) {
            this.telemetryClientId = telemetryClientId == null ? "" : telemetryClientId;
            return this;
        }

        /**
        * Sets the telemetry runtime state to be restored on the next client construction.
        *
        * @param telemetryRuntimeState the telemetry runtime state
        * @return this builder
        */
        public ConnectConfigBuilder telemetryRuntimeState(
                ClientTelemetryManager.RuntimeState telemetryRuntimeState) {
            this.telemetryRuntimeState = telemetryRuntimeState;
            return this;
        }

        /**
         * Builds the connection and telemetry stub without starting the telemetry worker.
         * Used only while a global-cluster replacement is prepared for an atomic handoff.
         */
        /**
        * Sets whether to defer starting the telemetry worker.
        *
        * @param deferTelemetryStart whether to defer starting the telemetry worker
        * @return this builder
        */
        public ConnectConfigBuilder deferTelemetryStart(boolean deferTelemetryStart) {
            this.deferTelemetryStart = deferTelemetryStart;
            return this;
        }

        /**
        * Sets additional connection options.
        *
        * @param option additional connection options
        * @return this builder
        */
        public ConnectConfigBuilder option(Map<String, String> option) {
            this.option = option;
            return this;
        }

        /**
        * Builds a {@link ConnectConfig}.
        *
        * @return the built configuration
        */
        public ConnectConfig build() {
            return new ConnectConfig(this);
        }
    }
}
