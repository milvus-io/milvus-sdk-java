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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;

/**
 * Periodically refreshes the global cluster topology and notifies listeners of changes.
 * <p>
 * The connection stub is the single owner of the shared topology: the refresher keeps no
 * private snapshot. {@code getCurrent} provides the authoritative copy whose version guards
 * each fetch, and every candidate update is handed to {@code onTopologyChange}, whose
 * compare-and-set decides whether the shared topology advances — so no refresh path can
 * regress the version.
 */

public class TopologyRefresher {
    private static final Logger logger = LoggerFactory.getLogger(TopologyRefresher.class);
    private static final long REFRESH_INTERVAL_MINUTES = 5;

    private final String globalEndpoint;
    private final String token;
    private final Supplier<GlobalTopology> getCurrent;
    private final Consumer<GlobalTopology> onTopologyChange;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean refreshing = new AtomicBoolean(false);

    public TopologyRefresher(String globalEndpoint, String token,
                             Supplier<GlobalTopology> getCurrent,
                             Consumer<GlobalTopology> onTopologyChange) {
        this.globalEndpoint = globalEndpoint;
        this.token = token;
        this.getCurrent = getCurrent;
        this.onTopologyChange = onTopologyChange;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "milvus-global-topology-refresher");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the periodic topology refresh.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(this::refresh, REFRESH_INTERVAL_MINUTES,
                REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES);
        logger.info("Global topology refresher started with {}min interval for endpoint: {}",
                REFRESH_INTERVAL_MINUTES, redactUriUserInfo(globalEndpoint));
    }

    /**
     * Triggers an immediate topology refresh outside the scheduled interval.
     * Refreshes already in progress are skipped.
     */
    public void triggerRefresh() {
        if (refreshing.getAndSet(true)) {
            logger.debug("Topology refresh already in progress, skipping");
            return;
        }
        try {
            scheduler.submit(this::refreshWithCleanup);
        } catch (Exception e) {
            refreshing.set(false);
            logger.warn("Failed to submit topology refresh task: {}", e.getMessage());
        }
    }

    /**
     * Stops the periodic topology refresh and shuts down the scheduler.
     */
    public void stop() {
        scheduler.shutdownNow();
        logger.info("Global topology refresher stopped for endpoint: {}", redactUriUserInfo(globalEndpoint));
    }

    private void refreshWithCleanup() {
        try {
            refresh();
        } finally {
            refreshing.set(false);
        }
    }

    private void refresh() {
        try {
            GlobalTopology current = getCurrent.get();
            Long cachedVersion = current == null ? null : current.getVersion();
            // fetchTopology returns null when no seed reports a strictly higher version,
            // so the shared topology can never regress through the refresh path.
            GlobalTopology newer = GlobalClusterUtils.fetchTopology(globalEndpoint, token,
                    cachedVersion, null);
            if (newer == null) {
                logger.debug("Global topology: no version newer than {} found, keeping cached topology",
                        cachedVersion);
                return;
            }
            logger.info("Global topology: discovered version {} (cached {}), triggering reconnection",
                    newer.getVersion(), cachedVersion);
            onTopologyChange.accept(newer);
        } catch (Exception e) {
            logger.warn("Failed to refresh global topology, keeping cached topology: {}", e.getMessage());
        }
    }
}
