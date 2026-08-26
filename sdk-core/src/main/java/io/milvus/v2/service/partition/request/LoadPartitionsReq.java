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

package io.milvus.v2.service.partition.request;

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code loadPartitions} API.
 */
public class LoadPartitionsReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private Integer numReplicas;
    private Boolean sync; // wait the partitions to be fully loaded
    private Long timeout; // timeout value for waiting the partitions to be fully loaded
    private Boolean refresh;
    private List<String> loadFields;
    private Boolean skipLoadDynamicField;
    private List<String> resourceGroups;

    private LoadPartitionsReq(LoadPartitionsReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.numReplicas = builder.numReplicas;
        this.sync = builder.sync;
        this.timeout = builder.timeout;
        this.refresh = builder.refresh;
        this.loadFields = builder.loadFields;
        this.skipLoadDynamicField = builder.skipLoadDynamicField;
        this.resourceGroups = builder.resourceGroups;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the names of the partitions to be loaded.
     *
     * @return the list of partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the names of the partitions to be loaded.
     *
     * @param partitionNames the list of partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    /**
     * Returns the number of replicas to create for the loaded partitions.
     *
     * @return the number of replicas
     */
    public Integer getNumReplicas() {
        return numReplicas;
    }

    /**
     * Sets the number of replicas to create for the loaded partitions.
     *
     * @param numReplicas the number of replicas
     */
    public void setNumReplicas(Integer numReplicas) {
        this.numReplicas = numReplicas;
    }

    /**
     * Returns whether the call waits until the partitions are fully loaded.
     *
     * @return {@code true} to wait for the partitions to be fully loaded
     */
    public Boolean getSync() {
        return sync;
    }

    /**
     * Sets whether the call waits until the partitions are fully loaded.
     *
     * @param sync {@code true} to wait for the partitions to be fully loaded
     */
    public void setSync(Boolean sync) {
        this.sync = sync;
    }

    /**
     * Returns the timeout in milliseconds for waiting the partitions to be fully loaded.
     *
     * @return the timeout value in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout in milliseconds for waiting the partitions to be fully loaded.
     *
     * @param timeout the timeout value in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    /**
     * Returns whether to refresh the load before loading the partitions.
     *
     * @return {@code true} to refresh the load
     */
    public Boolean getRefresh() {
        return refresh;
    }

    /**
     * Sets whether to refresh the load before loading the partitions.
     *
     * @param refresh {@code true} to refresh the load
     */
    public void setRefresh(Boolean refresh) {
        this.refresh = refresh;
    }

    /**
     * Returns the fields to be loaded for the partitions.
     *
     * @return the list of field names
     */
    public List<String> getLoadFields() {
        return loadFields;
    }

    /**
     * Sets the fields to be loaded for the partitions.
     *
     * @param loadFields the list of field names
     */
    public void setLoadFields(List<String> loadFields) {
        this.loadFields = loadFields;
    }

    /**
     * Returns whether to skip loading the dynamic field.
     *
     * @return {@code true} to skip loading the dynamic field
     */
    public Boolean getSkipLoadDynamicField() {
        return skipLoadDynamicField;
    }

    /**
     * Sets whether to skip loading the dynamic field.
     *
     * @param skipLoadDynamicField {@code true} to skip loading the dynamic field
     */
    public void setSkipLoadDynamicField(Boolean skipLoadDynamicField) {
        this.skipLoadDynamicField = skipLoadDynamicField;
    }

    /**
     * Returns the resource groups to which the partitions are loaded.
     *
     * @return the list of resource group names
     */
    public List<String> getResourceGroups() {
        return resourceGroups;
    }

    /**
     * Sets the resource groups to which the partitions are loaded.
     *
     * @param resourceGroups the list of resource group names
     */
    public void setResourceGroups(List<String> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public String toString() {
        return "LoadPartitionsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", numReplicas=" + numReplicas +
                ", sync=" + sync +
                ", timeout=" + timeout +
                ", refresh=" + refresh +
                ", loadFields=" + loadFields +
                ", skipLoadDynamicField=" + skipLoadDynamicField +
                ", resourceGroups=" + resourceGroups +
                '}';
    }

    /**
     * Creates a new builder for {@code LoadPartitionsReq}.
     *
     * @return the builder
     */
    public static LoadPartitionsReqBuilder builder() {
        return new LoadPartitionsReqBuilder();
    }

    public static class LoadPartitionsReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();
        private Integer numReplicas = 1;
        private Boolean sync = Boolean.TRUE; // wait the partitions to be fully loaded
        private Long timeout = 60000L; // timeout value for waiting the partitions to be fully loaded
        private Boolean refresh = Boolean.FALSE;
        private List<String> loadFields = new ArrayList<>();
        private Boolean skipLoadDynamicField = Boolean.FALSE;
        private List<String> resourceGroups = new ArrayList<>();

        private LoadPartitionsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public LoadPartitionsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public LoadPartitionsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the names of the partitions to be loaded.
         *
         * @param partitionNames the list of partition names
         * @return this builder
         */
        public LoadPartitionsReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the number of replicas to create for the loaded partitions.
         *
         * @param numReplicas the number of replicas
         * @return this builder
         */
        public LoadPartitionsReqBuilder numReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        /**
         * Sets whether the call waits until the partitions are fully loaded.
         *
         * @param sync {@code true} to wait for the partitions to be fully loaded
         * @return this builder
         */
        public LoadPartitionsReqBuilder sync(Boolean sync) {
            this.sync = sync;
            return this;
        }

        /**
         * Sets the timeout in milliseconds for waiting the partitions to be fully loaded.
         *
         * @param timeout the timeout value in milliseconds
         * @return this builder
         */
        public LoadPartitionsReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets whether to refresh the load before loading the partitions.
         *
         * @param refresh {@code true} to refresh the load
         * @return this builder
         */
        public LoadPartitionsReqBuilder refresh(Boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        /**
         * Sets the fields to be loaded for the partitions.
         *
         * @param loadFields the list of field names
         * @return this builder
         */
        public LoadPartitionsReqBuilder loadFields(List<String> loadFields) {
            this.loadFields = loadFields;
            return this;
        }

        /**
         * Sets whether to skip loading the dynamic field.
         *
         * @param skipLoadDynamicField {@code true} to skip loading the dynamic field
         * @return this builder
         */
        public LoadPartitionsReqBuilder skipLoadDynamicField(Boolean skipLoadDynamicField) {
            this.skipLoadDynamicField = skipLoadDynamicField;
            return this;
        }

        /**
         * Sets the resource groups to which the partitions are loaded.
         *
         * @param resourceGroups the list of resource group names
         * @return this builder
         */
        public LoadPartitionsReqBuilder resourceGroups(List<String> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        /**
         * Builds the {@code LoadPartitionsReq}.
         *
         * @return the built request
         */
        public LoadPartitionsReq build() {
            return new LoadPartitionsReq(this);
        }
    }
}
