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
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX;

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
    private boolean enablePrecheck = false;  // default value is false

    private SSLContext sslContext;
    // clientRequestId maintains a map for different threads, each thread can assign a specific id.
    // the specific id is passed to the server, from the access log we can know which client calls the interface
    private ThreadLocal<String> clientRequestId;

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
        this.enablePrecheck = builder.enablePrecheck;
    }

    public static ConnectConfigBuilder builder() {
        return new ConnectConfigBuilder();
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

    public ThreadLocal<String> getClientRequestId() {
        return clientRequestId;
    }

    public String getProxyAddress() {
        return proxyAddress;
    }

    public boolean isEnablePrecheck() {
    return enablePrecheck;
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

    public void setEnablePrecheck(boolean enablePrecheck) {
        this.enablePrecheck = enablePrecheck;
    }

    public void setIdleTimeoutMs(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public void setClientRequestId(ThreadLocal<String> clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    public String getHost() {
        URLParser urlParser = new URLParser(this.uri);
        return urlParser.getHostname();
    }

    public int getPort() {
        URLParser urlParser = new URLParser(this.uri);
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
        URLParser urlParser = new URLParser(this.uri);
        return StringUtils.isNotEmpty(urlParser.getDatabase()) ? urlParser.getDatabase() : this.dbName;
    }

    public Boolean isSecure() {
        if (uri.startsWith("https")) {
            return true;
        }
        return secure;
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
        private ThreadLocal<String> clientRequestId;
        private boolean enablePrecheck = false;

        public ConnectConfigBuilder uri(String uri) {
            if (uri == null) {
                throw new NullPointerException("uri is marked non-null but is null");
            }
            this.uri = uri;
            return this;
        }

        public ConnectConfigBuilder token(String token) {
            this.token = token;
            return this;
        }

        public ConnectConfigBuilder username(String username) {
            this.username = username;
            return this;
        }

        public ConnectConfigBuilder password(String password) {
            this.password = password;
            return this;
        }

        public ConnectConfigBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public ConnectConfigBuilder connectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public ConnectConfigBuilder keepAliveTimeMs(long keepAliveTimeMs) {
            this.keepAliveTimeMs = keepAliveTimeMs;
            return this;
        }

        public ConnectConfigBuilder keepAliveTimeoutMs(long keepAliveTimeoutMs) {
            this.keepAliveTimeoutMs = keepAliveTimeoutMs;
            return this;
        }

        public ConnectConfigBuilder keepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;
            return this;
        }

        public ConnectConfigBuilder rpcDeadlineMs(long rpcDeadlineMs) {
            this.rpcDeadlineMs = rpcDeadlineMs;
            return this;
        }

        public ConnectConfigBuilder clientKeyPath(String clientKeyPath) {
            this.clientKeyPath = clientKeyPath;
            return this;
        }

        public ConnectConfigBuilder clientPemPath(String clientPemPath) {
            this.clientPemPath = clientPemPath;
            return this;
        }

        public ConnectConfigBuilder caPemPath(String caPemPath) {
            this.caPemPath = caPemPath;
            return this;
        }

        public ConnectConfigBuilder serverPemPath(String serverPemPath) {
            this.serverPemPath = serverPemPath;
            return this;
        }

        public ConnectConfigBuilder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public ConnectConfigBuilder proxyAddress(String proxyAddress) {
            this.proxyAddress = proxyAddress;
            return this;
        }

        public ConnectConfigBuilder secure(Boolean secure) {
            this.secure = secure;
            return this;
        }

        public ConnectConfigBuilder enablePrecheck(boolean enablePrecheck) {
            this.enablePrecheck = enablePrecheck;
            return this;
        }

        public ConnectConfigBuilder idleTimeoutMs(long idleTimeoutMs) {
            this.idleTimeoutMs = idleTimeoutMs;
            return this;
        }

        public ConnectConfigBuilder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public ConnectConfigBuilder clientRequestId(ThreadLocal<String> clientRequestId) {
            this.clientRequestId = clientRequestId;
            return this;
        }

        public ConnectConfig build() {
            return new ConnectConfig(this);
        }
    }
}
