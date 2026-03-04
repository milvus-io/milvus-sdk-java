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

package io.milvus.v2.client.globalcluster;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class GlobalStub {
    private static final Logger logger = LoggerFactory.getLogger(GlobalStub.class);

    private final String globalEndpoint;
    private final ConnectConfig originalConfig;
    private final Consumer<MilvusClientV2> onPrimaryChange;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile MilvusClientV2 innerClient;
    private volatile String primaryEndpoint;
    private volatile GlobalTopology topology;
    private TopologyRefresher refresher;

    public GlobalStub(String globalEndpoint, ConnectConfig originalConfig,
                      Consumer<MilvusClientV2> onPrimaryChange) {
        this.globalEndpoint = globalEndpoint;
        this.originalConfig = originalConfig;
        this.onPrimaryChange = onPrimaryChange;

        // Fetch initial topology and connect to primary
        String authorization = originalConfig.getAuthorization();
        this.topology = GlobalClusterUtils.fetchTopology(globalEndpoint, authorization);
        ClusterInfo primary = this.topology.getPrimary();
        this.primaryEndpoint = primary.getEndpoint();
        logger.info("Global cluster: discovered primary endpoint: {}", primaryEndpoint);

        this.innerClient = createClientForEndpoint(primaryEndpoint);

        // Start background refresher
        this.refresher = new TopologyRefresher(globalEndpoint, authorization,
                topology.getVersion(), this::onTopologyChange);
        this.refresher.start();
    }

    public MilvusClientV2 getPrimaryClient() {
        return innerClient;
    }

    public GlobalTopology getTopology() {
        return topology;
    }

    public String getPrimaryEndpoint() {
        return primaryEndpoint;
    }

    public void triggerRefresh() {
        if (refresher != null) {
            refresher.triggerRefresh();
        }
    }

    public void close() {
        lock.lock();
        try {
            if (refresher != null) {
                refresher.stop();
                refresher = null;
            }
            if (innerClient != null) {
                innerClient.close();
                innerClient = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private void onTopologyChange(GlobalTopology newTopology) {
        lock.lock();
        try {
            ClusterInfo newPrimary = newTopology.getPrimary();
            String newEndpoint = newPrimary.getEndpoint();

            if (newEndpoint.equals(this.primaryEndpoint)) {
                logger.info("Global cluster: topology version changed but primary endpoint unchanged: {}", newEndpoint);
                this.topology = newTopology;
                return;
            }

            logger.info("Global cluster: primary endpoint changed from {} to {}", this.primaryEndpoint, newEndpoint);

            MilvusClientV2 oldClient = this.innerClient;
            MilvusClientV2 newClient = createClientForEndpoint(newEndpoint);

            this.innerClient = newClient;
            this.primaryEndpoint = newEndpoint;
            this.topology = newTopology;

            // Notify the outer client to swap its stub/channel
            onPrimaryChange.accept(newClient);

            // Close old client
            if (oldClient != null) {
                try {
                    oldClient.close();
                } catch (Exception e) {
                    logger.warn("Failed to close old primary client: {}", e.getMessage());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private MilvusClientV2 createClientForEndpoint(String endpoint) {
        ConnectConfig primaryConfig = cloneConfigWithNewUri(originalConfig, endpoint);
        return new MilvusClientV2(primaryConfig);
    }

    private static ConnectConfig cloneConfigWithNewUri(ConnectConfig original, String newUri) {
        // Construct the full URI for the primary endpoint
        // The endpoint from topology is typically just a hostname or hostname:port
        // We need to preserve the scheme (https) from the original URI
        String uri = newUri;
        if (!uri.startsWith("http://") && !uri.startsWith("https://")) {
            if (original.isSecure()) {
                uri = "https://" + uri;
            } else {
                uri = "http://" + uri;
            }
        }

        ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder()
                .uri(uri)
                .token(original.getToken())
                .dbName(original.getDbName());

        // Copy username/password if set (username builder validates non-null/non-blank)
        if (original.getUsername() != null) {
            builder.username(original.getUsername());
        }
        if (original.getPassword() != null) {
            builder.password(original.getPassword());
        }

        return builder
                .connectTimeoutMs(original.getConnectTimeoutMs())
                .keepAliveTimeMs(original.getKeepAliveTimeMs())
                .keepAliveTimeoutMs(original.getKeepAliveTimeoutMs())
                .keepAliveWithoutCalls(original.isKeepAliveWithoutCalls())
                .rpcDeadlineMs(original.getRpcDeadlineMs())
                .clientKeyPath(original.getClientKeyPath())
                .clientPemPath(original.getClientPemPath())
                .caPemPath(original.getCaPemPath())
                .serverPemPath(original.getServerPemPath())
                .serverName(original.getServerName())
                .proxyAddress(original.getProxyAddress())
                .secure(original.getSecure())
                .idleTimeoutMs(original.getIdleTimeoutMs())
                .sslContext(original.getSslContext())
                .clientRequestId(original.getClientRequestId())
                .enablePrecheck(original.isEnablePrecheck())
                .build();
    }
}
