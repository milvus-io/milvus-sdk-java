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

import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;

public class GlobalStub {
    private static final Logger logger = LoggerFactory.getLogger(GlobalStub.class);

    private final String globalEndpoint;
    private final ConnectConfig originalConfig;
    private final Consumer<MilvusClientV2> onPrimaryChange;
    private final ClientFactory clientFactory;
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
        this.clientFactory = this::createClientForEndpoint;

        // Fetch initial topology and connect to primary
        String authorization = originalConfig.getAuthorization();
        this.topology = GlobalClusterUtils.fetchTopology(globalEndpoint, authorization);
        ClusterInfo primary = this.topology.getPrimary();
        this.primaryEndpoint = primary.getEndpoint();
        logger.info("Global cluster: discovered primary endpoint: {}", redactUriUserInfo(primaryEndpoint));

        this.innerClient = clientFactory.create(
                primaryEndpoint, originalConfig.takeTelemetryRuntimeState(), false);

        // Start background refresher
        this.refresher = new TopologyRefresher(globalEndpoint, authorization,
                topology.getVersion(), this::onTopologyChange);
        this.refresher.start();
    }

    GlobalStub(String globalEndpoint, ConnectConfig originalConfig,
               Consumer<MilvusClientV2> onPrimaryChange, GlobalTopology topology,
               MilvusClientV2 innerClient, ClientFactory clientFactory) {
        this.globalEndpoint = globalEndpoint;
        this.originalConfig = originalConfig;
        this.onPrimaryChange = onPrimaryChange;
        this.topology = topology;
        this.innerClient = innerClient;
        this.primaryEndpoint = topology.getPrimary().getEndpoint();
        this.clientFactory = clientFactory;
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

    void onTopologyChange(GlobalTopology newTopology) {
        lock.lock();
        try {
            ClusterInfo newPrimary = newTopology.getPrimary();
            String newEndpoint = newPrimary.getEndpoint();

            if (newEndpoint.equals(this.primaryEndpoint)) {
                logger.info("Global cluster: topology version changed but primary endpoint unchanged: {}",
                        redactUriUserInfo(newEndpoint));
                this.topology = newTopology;
                return;
            }

            logger.info("Global cluster: primary endpoint changed from {} to {}",
                    redactUriUserInfo(this.primaryEndpoint), redactUriUserInfo(newEndpoint));

            MilvusClientV2 oldClient = this.innerClient;
            ClientTelemetryManager oldTelemetry = oldClient == null ? null : oldClient.getTelemetry();
            ClientTelemetryManager.RuntimeState telemetrySeed = oldTelemetry == null
                    ? null : oldTelemetry.snapshotRuntimeState();
            MilvusClientV2 newClient = clientFactory.create(newEndpoint, telemetrySeed, true);
            ClientTelemetryManager newTelemetry = newClient.getTelemetry();
            String oldEndpoint = this.primaryEndpoint;
            GlobalTopology oldTopology = this.topology;
            long retirementToken = 0L;
            boolean retirementPending = false;
            try {
                if (oldTelemetry != null) {
                    if (newTelemetry == null) {
                        throw new IllegalStateException("replacement client has no telemetry manager");
                    }
                    // Calls that begin after the outer stub switch but before the final state
                    // transfer are redirected into the still-active old manager.
                    newTelemetry.prepareRuntimeStateHandoffFrom(oldTelemetry);
                    // Fence the old endpoint before publishing the replacement. A heartbeat
                    // already in flight may finish later, but its response must not mutate the
                    // runtime state that will be handed to the new primary.
                    retirementToken = oldTelemetry.beginRuntimeStateRetirement();
                    retirementPending = true;
                }

                this.innerClient = newClient;
                this.primaryEndpoint = newEndpoint;
                this.topology = newTopology;
                onPrimaryChange.accept(newClient);

                if (oldTelemetry != null) {
                    oldTelemetry.handoffRuntimeStateTo(newTelemetry, retirementToken);
                    retirementPending = false;
                } else {
                    newClient.startTelemetry();
                }
            } catch (RuntimeException | Error exception) {
                this.innerClient = oldClient;
                this.primaryEndpoint = oldEndpoint;
                this.topology = oldTopology;
                if (newTelemetry != null && oldTelemetry != null) {
                    newTelemetry.cancelRuntimeStateHandoffFrom(oldTelemetry);
                }
                if (retirementPending && oldTelemetry != null) {
                    oldTelemetry.cancelRuntimeStateRetirement(retirementToken);
                }
                if (oldClient != null) {
                    try {
                        onPrimaryChange.accept(oldClient);
                    } catch (RuntimeException | Error rollbackException) {
                        exception.addSuppressed(rollbackException);
                    }
                }
                try {
                    newClient.close();
                } catch (Exception closeException) {
                    exception.addSuppressed(closeException);
                }
                throw exception;
            }

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

    private MilvusClientV2 createClientForEndpoint(
            String endpoint, ClientTelemetryManager.RuntimeState telemetryRuntimeState,
            boolean deferTelemetryStart) {
        ConnectConfig primaryConfig = cloneConfigWithNewUri(
                originalConfig, endpoint, telemetryRuntimeState, deferTelemetryStart);
        return new MilvusClientV2(primaryConfig);
    }

    private static ConnectConfig cloneConfigWithNewUri(
            ConnectConfig original,
            String newUri,
            ClientTelemetryManager.RuntimeState telemetryRuntimeState,
            boolean deferTelemetryStart) {
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
                .telemetryConfig(original.getTelemetryConfig())
                .telemetryClientId(original.getTelemetryClientId())
                .telemetryRuntimeState(telemetryRuntimeState)
                .deferTelemetryStart(deferTelemetryStart)
                .enablePrecheck(original.isEnablePrecheck())
                .build();
    }

    @FunctionalInterface
    interface ClientFactory {
        MilvusClientV2 create(String endpoint,
                              ClientTelemetryManager.RuntimeState telemetryRuntimeState,
                              boolean deferTelemetryStart);
    }
}
