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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;

/**
 * Zilliz Cloud only. Manages the client connection to the primary cluster of a global cluster deployment.
 * <p>
 * On construction the stub fetches the global topology and connects to the primary cluster.
 * Background updates begin only after the owning client calls {@code activate()}. When the
 * topology changes, the stub swaps to a new client for the new primary endpoint and notifies
 * the registered primary-change callback.
 *
 * @zillizCloudOnly
 */


public class GlobalStub {
    private static final Logger logger = LoggerFactory.getLogger(GlobalStub.class);

    private final String globalEndpoint;
    private final ConnectConfig originalConfig;
    private volatile Consumer<MilvusClientV2> onPrimaryChange;
    private final ClientFactory clientFactory;
    private final DeferredTopologyCallback initialTopologyCallback;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile MilvusClientV2 innerClient;
    private volatile String primaryEndpoint;
    private volatile GlobalTopology topology;
    private volatile boolean closed;
    private volatile TopologyRefresher refresher;
    private boolean activated;

    /**
     * Creates a {@code GlobalStub} for the given global cluster endpoint, fetching the initial
     * topology and connecting to the current primary cluster. Background updates remain gated
     * until the owning client completes initialization and activates the stub.
     *
     * @param globalEndpoint the global cluster endpoint
     * @param originalConfig the original connection configuration
     * @param onPrimaryChange the consumer notified when the primary cluster changes
     */
    public GlobalStub(String globalEndpoint, ConnectConfig originalConfig,
                      Consumer<MilvusClientV2> onPrimaryChange) {
        this(globalEndpoint, originalConfig, onPrimaryChange,
                GlobalClusterUtils::fetchTopology, null);
    }

    GlobalStub(String globalEndpoint, ConnectConfig originalConfig,
               Consumer<MilvusClientV2> onPrimaryChange, TopologyFetcher topologyFetcher,
               ClientFactory clientFactory) {
        this.globalEndpoint = globalEndpoint;
        this.originalConfig = originalConfig;
        this.onPrimaryChange = onPrimaryChange;
        this.clientFactory = clientFactory == null ? this::createClientForEndpoint : clientFactory;
        this.initialTopologyCallback = new DeferredTopologyCallback();

        String authorization = originalConfig.getAuthorization();
        try {
            GlobalTopology first = topologyFetcher.fetch(globalEndpoint, authorization,
                    null, initialTopologyCallback);
            ClusterInfo primary = first.getPrimary();
            this.topology = first;
            this.primaryEndpoint = primary.getEndpoint();
            logger.info("Global cluster: discovered primary endpoint: {}",
                    redactUriUserInfo(primaryEndpoint));
            this.innerClient = this.clientFactory.create(primaryEndpoint,
                    originalConfig.takeTelemetryRuntimeState(),
                    originalConfig.isDeferTelemetryStart());
        } catch (RuntimeException | Error exception) {
            initialTopologyCallback.close();
            throw exception;
        }
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
        this.initialTopologyCallback = new DeferredTopologyCallback();
    }

    /**
     * Starts background topology updates after the owning client has been fully published.
     * This method is idempotent; calls after close are ignored.
     */
    public void activate() {
        lock.lock();
        try {
            if (closed || activated) {
                return;
            }
            TopologyRefresher created = new TopologyRefresher(globalEndpoint,
                    originalConfig.getAuthorization(), this::getTopology, this::onTopologyChange);
            try {
                created.start();
            } catch (RuntimeException | Error exception) {
                created.stop();
                throw exception;
            }
            this.refresher = created;
            this.activated = true;
            initialTopologyCallback.activate(this::onTopologyChange);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the client connected to the current primary cluster.
     *
     * @return the primary client
     */


    public MilvusClientV2 getPrimaryClient() {
        return innerClient;
    }

    /**
     * Returns the most recently fetched global topology.
     *
     * @return the current topology
     */


    public GlobalTopology getTopology() {
        return topology;
    }

    /**
     * Returns the endpoint of the current primary cluster.
     *
     * @return the primary endpoint
     */


    public String getPrimaryEndpoint() {
        return primaryEndpoint;
    }

    /**
     * Triggers an immediate topology refresh, which may swap the primary client.
     */


    public void triggerRefresh() {
        if (refresher != null) {
            refresher.triggerRefresh();
        }
    }

    /**
     * Retargets primary-change publication when a prepared global connection is adopted,
     * and publishes the current primary under the same topology lock.
     * @param callback the consumer notified when the primary cluster changes
     */


    public void retargetPrimaryChange(Consumer<MilvusClientV2> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("primary-change callback is required");
        }
        lock.lock();
        try {
            Consumer<MilvusClientV2> previous = this.onPrimaryChange;
            this.onPrimaryChange = callback;
            try {
                callback.accept(innerClient);
            } catch (RuntimeException | Error exception) {
                this.onPrimaryChange = previous;
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops the topology refresher and closes the current primary client.
     */


    public void close() {
        lock.lock();
        try {
            closed = true;
            initialTopologyCallback.close();
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

    /**
     * Installs a candidate topology only if it strictly advances the version, swapping the
     * primary client when the primary endpoint moved.
     * <p>
     * Every write to the shared topology funnels through this compare-and-set, so no update
     * path (initial-fetch watcher, periodic refresh, UNAVAILABLE-triggered refresh) can roll
     * the version back, however the threads interleave. A failed client swap rolls back to
     * the previous topology, which the next refresh will retry.
     *
     * @param newTopology the candidate topology
     * @return whether the candidate was accepted
     */
    boolean onTopologyChange(GlobalTopology newTopology) {
        lock.lock();
        try {
            if (closed) {
                logger.debug("Global cluster: ignoring topology update after close (version {})",
                        newTopology.getVersion());
                return false;
            }
            if (this.topology != null && newTopology.getVersion() <= this.topology.getVersion()) {
                logger.debug("Global cluster: ignored stale topology version {} (current {})",
                        newTopology.getVersion(), this.topology.getVersion());
                return false;
            }

            ClusterInfo newPrimary = newTopology.getPrimary();
            String newEndpoint = newPrimary.getEndpoint();
            String newConnectionUri = normalizePrimaryUri(originalConfig, newEndpoint);
            String currentConnectionUri = this.primaryEndpoint == null
                    ? null : normalizePrimaryUri(originalConfig, this.primaryEndpoint);

            if (newConnectionUri.equals(currentConnectionUri)) {
                logger.info("Global cluster: topology version changed but primary endpoint unchanged: {}",
                        redactUriUserInfo(newEndpoint));
                this.primaryEndpoint = newEndpoint;
                this.topology = newTopology;
                return true;
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
            return true;
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

    private static String normalizePrimaryUri(ConnectConfig original, String endpoint) {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Primary endpoint is required");
        }
        String value = endpoint.trim();
        if (!value.matches("(?i)^https?://.*")) {
            if (value.contains("://")) {
                throw new IllegalArgumentException("Unsupported primary endpoint scheme: " + endpoint);
            }
            value = (original.isSecure() ? "https://" : "http://") + value;
        }
        try {
            URI parsed = new URI(value);
            String scheme = parsed.getScheme().toLowerCase(Locale.ROOT);
            String host = parsed.getHost();
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("Invalid primary endpoint: " + endpoint);
            }
            int port = parsed.getPort();
            if (port <= 0) {
                port = "https".equals(scheme) ? 443 : 19530;
            }
            return new URI(scheme, null, host.toLowerCase(Locale.ROOT), port,
                    null, null, null).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid primary endpoint: " + endpoint, e);
        }
    }

    private static ConnectConfig cloneConfigWithNewUri(
            ConnectConfig original,
            String newUri,
            ClientTelemetryManager.RuntimeState telemetryRuntimeState,
            boolean deferTelemetryStart) {
        String uri = normalizePrimaryUri(original, newUri);

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
                .secure(uri.startsWith("https://"))
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
