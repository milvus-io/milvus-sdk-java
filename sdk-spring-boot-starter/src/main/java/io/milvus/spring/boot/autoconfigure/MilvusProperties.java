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

package io.milvus.spring.boot.autoconfigure;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Milvus client, bound to the {@code milvus.*} namespace.
 * <p>
 * Properties that are left unset or blank fall back to the defaults of {@link io.milvus.v2.client.ConnectConfig}.
 * The only exception is {@link #getUri() uri}: {@code ConnectConfig} has no default URI, so the starter defaults to
 * {@value #DEFAULT_URI}.
 */
@ConfigurationProperties(prefix = "milvus")
public class MilvusProperties {

    /**
     * Default URI: the address of a local Milvus standalone server. {@code ConnectConfig} requires a URI and has
     * no default of its own, so this is the single place the starter defines one.
     */
    public static final String DEFAULT_URI = "http://localhost:19530";

    /**
     * Whether to enable the Milvus auto-configuration.
     */
    private boolean enabled = true;

    /**
     * URI of the Milvus server. Defaults to a local Milvus standalone server.
     */
    private String uri = DEFAULT_URI;

    /**
     * Authentication token, for example {@code root:Milvus} or a Zilliz Cloud API key.
     */
    private String token;

    /**
     * Username for authentication.
     */
    private String username;

    /**
     * Password for authentication.
     */
    private String password;

    /**
     * Name of the database to connect to.
     */
    private String dbName;

    /**
     * Timeout for establishing the connection.
     */
    private Duration connectTimeout;

    /**
     * Interval between keep-alive pings.
     */
    private Duration keepAliveTime;

    /**
     * Time to wait for a keep-alive ping acknowledgement.
     */
    private Duration keepAliveTimeout;

    /**
     * Whether to send keep-alive pings when there are no outstanding calls.
     */
    private Boolean keepAliveWithoutCalls;

    /**
     * Deadline for each RPC call. Zero disables the deadline.
     */
    private Duration rpcDeadline;

    /**
     * Idle time after which the connection is closed.
     */
    private Duration idleTimeout;

    /**
     * Whether to use a secure (TLS) connection.
     */
    private Boolean secure;

    /**
     * Expected server name for TLS verification.
     */
    private String serverName;

    /**
     * Path to the server certificate for one-way TLS.
     */
    private String serverPemPath;

    /**
     * Path to the CA certificate for mutual TLS.
     */
    private String caPemPath;

    /**
     * Path to the client certificate for mutual TLS.
     */
    private String clientPemPath;

    /**
     * Path to the client private key for mutual TLS.
     */
    private String clientKeyPath;

    /**
     * Address of a proxy to connect through.
     */
    private String proxyAddress;

    /**
     * Whether to check that the server is ready when the client connects.
     */
    private Boolean enablePrecheck;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(Duration keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public Duration getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(Duration keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public Boolean getKeepAliveWithoutCalls() {
        return keepAliveWithoutCalls;
    }

    public void setKeepAliveWithoutCalls(Boolean keepAliveWithoutCalls) {
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    public Duration getRpcDeadline() {
        return rpcDeadline;
    }

    public void setRpcDeadline(Duration rpcDeadline) {
        this.rpcDeadline = rpcDeadline;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Boolean getSecure() {
        return secure;
    }

    public void setSecure(Boolean secure) {
        this.secure = secure;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerPemPath() {
        return serverPemPath;
    }

    public void setServerPemPath(String serverPemPath) {
        this.serverPemPath = serverPemPath;
    }

    public String getCaPemPath() {
        return caPemPath;
    }

    public void setCaPemPath(String caPemPath) {
        this.caPemPath = caPemPath;
    }

    public String getClientPemPath() {
        return clientPemPath;
    }

    public void setClientPemPath(String clientPemPath) {
        this.clientPemPath = clientPemPath;
    }

    public String getClientKeyPath() {
        return clientKeyPath;
    }

    public void setClientKeyPath(String clientKeyPath) {
        this.clientKeyPath = clientKeyPath;
    }

    public String getProxyAddress() {
        return proxyAddress;
    }

    public void setProxyAddress(String proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    public Boolean getEnablePrecheck() {
        return enablePrecheck;
    }

    public void setEnablePrecheck(Boolean enablePrecheck) {
        this.enablePrecheck = enablePrecheck;
    }
}
