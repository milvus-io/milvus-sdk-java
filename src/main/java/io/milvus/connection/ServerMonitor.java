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

package io.milvus.connection;

import io.milvus.param.QueryNodeSingleSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Monitor with scheduling to check server healthy state.
 */
public class ServerMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ServerMonitor.class);

    private static final long heartbeatInterval = 10 * 1000;

    private Long lastHeartbeat;

    private final List<Listener> listeners;

    private final ClusterFactory clusterFactory;

    private final Thread monitorThread;
    private volatile boolean isRunning;

    public ServerMonitor(ClusterFactory clusterFactory, QueryNodeSingleSearch queryNodeSingleSearch) {
        if (null != queryNodeSingleSearch) {
            this.listeners = Arrays.asList(new ClusterListener(), new QueryNodeListener(queryNodeSingleSearch));
        } else {
            this.listeners = Collections.singletonList(new ClusterListener());
        }
        this.clusterFactory = clusterFactory;

        ServerMonitorRunnable monitor = new ServerMonitorRunnable();
        this.monitorThread = new Thread(monitor, "Milvus-server-monitor");
        this.monitorThread.setDaemon(true);
        this.isRunning = true;
    }

    public void start() {
        logger.info("Milvus Server Monitor start.");
        monitorThread.start();
    }

    public void close() {
        isRunning = false;
        logger.info("Milvus Server Monitor close.");
        monitorThread.interrupt();
    }

    private class ServerMonitorRunnable implements Runnable {
        public void run() {
            while (isRunning) {
                long startTime = System.currentTimeMillis();
                long timeTillNextCheck;

                if (null == lastHeartbeat || (timeTillNextCheck = lastHeartbeat + heartbeatInterval - startTime) <= 0) {

                    lastHeartbeat = startTime;

                    try {
                        List<ServerSetting> availableServer = getAvailableServer();
                        clusterFactory.availableServerChange(availableServer);
                    } catch (Exception e) {
                        logger.error("Milvus Server Heartbeat error, monitor will stop.", e);
                    }

                    if (!clusterFactory.masterIsRunning()) {
                        ServerSetting master = clusterFactory.electMaster();

                        logger.warn("Milvus Server Heartbeat. Master is Not Running, Re-Elect [{}] to master.",
                                master.getServerAddress().getHost());

                        clusterFactory.masterChange(master);
                    } else {
                        logger.debug("Milvus Server Heartbeat. Master is Running.");
                    }
                } else {
                    try {
                        Thread.sleep(timeTillNextCheck);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Milvus Server Heartbeat. Interrupted.");
                        throw new RuntimeException(e);
                    }
                }
            }

        }

        private List<ServerSetting> getAvailableServer() {
            return clusterFactory.getServerSettings().stream()
                    .filter(this::checkServerState).collect(Collectors.toList());
        }

        private boolean checkServerState(ServerSetting serverSetting) {
            for (Listener listener : listeners) {
                boolean isRunning = listener.heartBeat(serverSetting);
                if (!isRunning) {
                    return false;
                }
            }
            return true;
        }
    }
}
