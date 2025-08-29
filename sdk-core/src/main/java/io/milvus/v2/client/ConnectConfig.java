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

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.net.ssl.SSLContext;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ConnectConfig {
    private String uri;
    private String token;
    private String username;
    private String password;
    private String dbName;
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
    private String proxyAddress;
    private Boolean secure = false;
    private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);

    private SSLContext sslContext;
    // clientRequestId maintains a map for different threads, each thread can assign a specific id.
    // the specific id is passed to the server, from the access log we can know which client calls the interface
    private ThreadLocal<String> clientRequestId;

    // Constructor for builder
    private ConnectConfig(Builder builder) {
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
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getUri() {
        return uri;
    }

    public String getToken() {
        return token;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
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

    public long getRpcDeadlineMs() {
        return rpcDeadlineMs;
    }

    public String getClientKeyPath() {
        return clientKeyPath;
    }

    public String getClientPemPath() {
        return clientPemPath;
    }

    public String getCaPemPath() {
        return caPemPath;
    }

    public String getServerPemPath() {
        return serverPemPath;
    }

    public String getServerName() {
        return serverName;
    }

    public Boolean getSecure() {
        return secure;
    }

    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    // Setters
    public void setUri(String uri) {
        if (uri == null) {
            throw new NullPointerException("uri is marked non-null but is null");
        }
        this.uri = uri;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setConnectTimeoutMs(long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public void setKeepAliveTimeMs(long keepAliveTimeMs) {
        this.keepAliveTimeMs = keepAliveTimeMs;
    }

    public void setKeepAliveTimeoutMs(long keepAliveTimeoutMs) {
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
    }

    public void setKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    public void setRpcDeadlineMs(long rpcDeadlineMs) {
        this.rpcDeadlineMs = rpcDeadlineMs;
    }

    public void setClientKeyPath(String clientKeyPath) {
        this.clientKeyPath = clientKeyPath;
    }

    public void setClientPemPath(String clientPemPath) {
        this.clientPemPath = clientPemPath;
    }

    public void setCaPemPath(String caPemPath) {
        this.caPemPath = caPemPath;
    }

    public void setServerPemPath(String serverPemPath) {
        this.serverPemPath = serverPemPath;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public void setProxyAddress(String proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    public void setSecure(Boolean secure) {
        this.secure = secure;
    }

    public void setIdleTimeoutMs(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public String getHost() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        return urlParser.getHostname();
    }

    public int getPort() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        int port = urlParser.getPort();
        if (Pattern.matches(CLOUD_SERVERLESS_URI_REGEX, this.uri)) {
            port = 443;
        }
        return port;
    }

    public String getAuthorization() {
        if (token != null) {
            return token;
        } else if (username != null && password != null) {
            return username + ":" + password;
        }
        return null;
    }

    public String getDbName() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        return StringUtils.isNotEmpty(urlParser.getDatabase()) ? urlParser.getDatabase() : this.dbName;
    }

    public Boolean isSecure() {
        if (uri.startsWith("https")) {
            return true;
        }
        return secure;
    }

    public String getProxyAddress() {
        return proxyAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectConfig that = (ConnectConfig) o;

        return new EqualsBuilder()
                .append(connectTimeoutMs, that.connectTimeoutMs)
                .append(keepAliveTimeMs, that.keepAliveTimeMs)
                .append(keepAliveTimeoutMs, that.keepAliveTimeoutMs)
                .append(keepAliveWithoutCalls, that.keepAliveWithoutCalls)
                .append(rpcDeadlineMs, that.rpcDeadlineMs)
                .append(idleTimeoutMs, that.idleTimeoutMs)
                .append(uri, that.uri)
                .append(token, that.token)
                .append(username, that.username)
                .append(password, that.password)
                .append(dbName, that.dbName)
                .append(clientKeyPath, that.clientKeyPath)
                .append(clientPemPath, that.clientPemPath)
                .append(caPemPath, that.caPemPath)
                .append(serverPemPath, that.serverPemPath)
                .append(serverName, that.serverName)
                .append(proxyAddress, that.proxyAddress)
                .append(secure, that.secure)
                .append(sslContext, that.sslContext)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(uri)
                .append(token)
                .append(username)
                .append(password)
                .append(dbName)
                .append(connectTimeoutMs)
                .append(keepAliveTimeMs)
                .append(keepAliveTimeoutMs)
                .append(keepAliveWithoutCalls)
                .append(rpcDeadlineMs)
                .append(clientKeyPath)
                .append(clientPemPath)
                .append(caPemPath)
                .append(serverPemPath)
                .append(serverName)
                .append(proxyAddress)
                .append(secure)
                .append(idleTimeoutMs)
                .append(sslContext)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "ConnectConfig{" +
                "uri='" + uri + '\'' +
                ", token='" + token + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
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
                ", idleTimeoutMs=" + idleTimeoutMs +
                ", sslContext=" + sslContext +
                '}';
    }

    public static class Builder {
        private String uri;
        private String token;
        private String username;
        private String password;
        private String dbName;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = 55000;
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
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

        public Builder uri(String uri) {
            if (uri == null) {
                throw new NullPointerException("uri is marked non-null but is null");
            }
            this.uri = uri;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder connectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder keepAliveTimeMs(long keepAliveTimeMs) {
            this.keepAliveTimeMs = keepAliveTimeMs;
            return this;
        }

        public Builder keepAliveTimeoutMs(long keepAliveTimeoutMs) {
            this.keepAliveTimeoutMs = keepAliveTimeoutMs;
            return this;
        }

        public Builder keepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;
            return this;
        }

        public Builder rpcDeadlineMs(long rpcDeadlineMs) {
            this.rpcDeadlineMs = rpcDeadlineMs;
            return this;
        }

        public Builder clientKeyPath(String clientKeyPath) {
            this.clientKeyPath = clientKeyPath;
            return this;
        }

        public Builder clientPemPath(String clientPemPath) {
            this.clientPemPath = clientPemPath;
            return this;
        }

        public Builder caPemPath(String caPemPath) {
            this.caPemPath = caPemPath;
            return this;
        }

        public Builder serverPemPath(String serverPemPath) {
            this.serverPemPath = serverPemPath;
            return this;
        }

        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public Builder proxyAddress(String proxyAddress) {
            this.proxyAddress = proxyAddress;
            return this;
        }

        public Builder secure(Boolean secure) {
            this.secure = secure;
            return this;
        }

        public Builder idleTimeoutMs(long idleTimeoutMs) {
            this.idleTimeoutMs = idleTimeoutMs;
            return this;
        }

        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public ConnectConfig build() {
            return new ConnectConfig(this);
        }
    }
}
