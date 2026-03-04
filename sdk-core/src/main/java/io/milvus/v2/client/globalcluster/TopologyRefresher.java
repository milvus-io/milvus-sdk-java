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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class TopologyRefresher {
    private static final Logger logger = LoggerFactory.getLogger(TopologyRefresher.class);
    private static final long REFRESH_INTERVAL_MINUTES = 5;

    private final String globalEndpoint;
    private final String token;
    private final Consumer<GlobalTopology> onTopologyChange;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong currentVersion;

    public TopologyRefresher(String globalEndpoint, String token, long initialVersion,
                             Consumer<GlobalTopology> onTopologyChange) {
        this.globalEndpoint = globalEndpoint;
        this.token = token;
        this.onTopologyChange = onTopologyChange;
        this.currentVersion = new AtomicLong(initialVersion);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "milvus-global-topology-refresher");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::refresh, REFRESH_INTERVAL_MINUTES,
                REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES);
        logger.info("Global topology refresher started with {}min interval for endpoint: {}",
                REFRESH_INTERVAL_MINUTES, globalEndpoint);
    }

    public void triggerRefresh() {
        scheduler.submit(this::refresh);
    }

    public void stop() {
        scheduler.shutdownNow();
        logger.info("Global topology refresher stopped for endpoint: {}", globalEndpoint);
    }

    private void refresh() {
        try {
            GlobalTopology topology = GlobalClusterUtils.fetchTopology(globalEndpoint, token);
            long newVersion = topology.getVersion();
            long oldVersion = currentVersion.get();
            if (newVersion != oldVersion) {
                logger.info("Global topology version changed from {} to {}, triggering reconnection", oldVersion, newVersion);
                currentVersion.set(newVersion);
                onTopologyChange.accept(topology);
            } else {
                logger.debug("Global topology version unchanged ({}), no action needed", oldVersion);
            }
        } catch (Exception e) {
            logger.warn("Failed to refresh global topology, keeping cached topology: {}", e.getMessage());
        }
    }
}
