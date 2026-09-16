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

import io.milvus.exception.ParamException;
import io.milvus.param.QueryNodeSingleSearch;
import io.milvus.param.ServerAddress;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Factory with managing multi cluster.
 */


public class ClusterFactory {

    private final List<ServerSetting> serverSettings;

    private ServerSetting master;

    private List<ServerSetting> availableServerSettings;

    private ServerMonitor monitor;

    private ClusterFactory(Builder builder) {
        if (builder == null) {
            throw new NullPointerException("builder cannot be null");
        }
        this.serverSettings = builder.serverSettings;
        this.master = this.getDefaultServer();
        this.availableServerSettings = builder.serverSettings;
        if (builder.keepMonitor) {
            monitor = new ServerMonitor(this, builder.queryNodeSingleSearch);
            monitor.start();
        }
    }

    /**
     * Returns the first configured server, used as the initial default.
     *
     * @return the default {@link ServerSetting}
     */


    public ServerSetting getDefaultServer() {
        return serverSettings.get(0);
    }

    /**
     * Checks whether the current master server is among the available servers.
     *
     * @return {@code true} if the master is running
     */


    public boolean masterIsRunning() {
        List<ServerAddress> serverAddresses = availableServerSettings.stream()
                .map(ServerSetting::getServerAddress)
                .collect(Collectors.toList());

        return serverAddresses.contains(master.getServerAddress());
    }

    /**
     * Sets the server to act as the master of the cluster.
     *
     * @param serverSetting the server to become the master
     */


    public void masterChange(ServerSetting serverSetting) {
        this.master = serverSetting;
    }

    /**
     * Updates the list of servers currently available.
     *
     * @param serverSettings the newly available servers
     */


    public void availableServerChange(List<ServerSetting> serverSettings) {
        this.availableServerSettings = serverSettings;
    }

    /**
     * Elects a master from the available servers, falling back to the default server.
     *
     * @return the elected {@link ServerSetting}
     */


    public ServerSetting electMaster() {
        return CollectionUtils.isNotEmpty(availableServerSettings) ? availableServerSettings.get(0) : getDefaultServer();
    }

    /**
     * Stops the server monitor, if one is running.
     */


    public void close() {
        if (null != monitor) {
            monitor.close();
        }
    }

    /**
     * Returns all configured servers of the cluster.
     *
     * @return the server settings
     */


    public List<ServerSetting> getServerSettings() {
        return serverSettings;
    }

    /**
     * Returns the current master server.
     *
     * @return the master {@link ServerSetting}
     */


    public ServerSetting getMaster() {
        return master;
    }

    /**
     * Returns the servers currently available.
     *
     * @return the available server settings
     */


    public List<ServerSetting> getAvailableServerSettings() {
        return availableServerSettings;
    }

    /**
     * Creates a new {@link ClusterFactory} builder.
     *
     * @return a new builder
     */


    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ClusterFactory}
     */


    public static class Builder {
        private List<ServerSetting> serverSettings;
        private boolean keepMonitor = false;
        private QueryNodeSingleSearch queryNodeSingleSearch;

        private Builder() {
        }

        /**
         * Sets server setting list
         *
         * @param serverSettings ServerSetting
         * @return <code>Builder</code>
         */


        public Builder withServerSetting(List<ServerSetting> serverSettings) {
            if (serverSettings == null) {
                throw new NullPointerException("serverSettings cannot be null");
            }
            this.serverSettings = serverSettings;
            return this;
        }

        /**
         * Enables the keep-monitor function for server
         *
         * @param enable true keep-monitor
         * @return <code>Builder</code>
         */


        public Builder keepMonitor(boolean enable) {
            this.keepMonitor = enable;
            return this;
        }

        /**
         * Sets single search for query node listener.
         *
         * @param queryNodeSingleSearch query node single search for listener
         * @return <code>Builder</code>
         */


        public Builder withQueryNodeSingleSearch(QueryNodeSingleSearch queryNodeSingleSearch) {
            this.queryNodeSingleSearch = queryNodeSingleSearch;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ClusterFactory} instance.
         *
         * @return {@link ClusterFactory}
         */


        public ClusterFactory build() throws ParamException {

            if (CollectionUtils.isEmpty(serverSettings)) {
                throw new ParamException("Server settings is empty!");
            }

            return new ClusterFactory(this);
        }
    }
}
