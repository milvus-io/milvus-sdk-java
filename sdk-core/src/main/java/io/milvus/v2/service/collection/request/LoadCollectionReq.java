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

package io.milvus.v2.service.collection.request;

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code loadCollection} API.
 */
public class LoadCollectionReq {
    private String databaseName;
    private String collectionName;
    private Integer numReplicas = 1;
    @Deprecated
    private Boolean async = Boolean.FALSE;
    private Boolean sync = Boolean.TRUE; // wait the collection to be fully loaded. "async" is deprecated, use "sync" instead
    private Long timeout = 60000L; // timeout value for waiting the collection to be fully loaded
    private Boolean refresh = Boolean.FALSE;
    private List<String> loadFields = new ArrayList<>();
    private Boolean skipLoadDynamicField = Boolean.FALSE;
    private List<String> resourceGroups = new ArrayList<>();

    private LoadCollectionReq(LoadCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.numReplicas = builder.numReplicas;
        this.async = builder.async;
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
     * Returns the number of replicas to load for the collection.
     *
     * @return the number of replicas
     */
    public Integer getNumReplicas() {
        return numReplicas;
    }

    /**
     * Sets the number of replicas to load for the collection.
     *
     * @param numReplicas the number of replicas
     */
    public void setNumReplicas(Integer numReplicas) {
        this.numReplicas = numReplicas;
    }

    /**
     * Returns whether the load operation is asynchronous.
     *
     * @return {@code true} if the operation is asynchronous
     * @deprecated use {@link #getSync()} instead
     */
    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    /**
     * Sets whether the load operation is asynchronous.
     *
     * @param async {@code true} to run the operation asynchronously
     * @deprecated use {@link #setSync(Boolean)} instead
     */
    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
        this.sync = !async;
    }

    /**
     * Returns whether to wait until the collection is fully loaded.
     *
     * @return {@code true} if the operation waits for the collection to be loaded
     */
    public Boolean getSync() {
        return sync;
    }

    /**
     * Sets whether to wait until the collection is fully loaded.
     *
     * @param sync {@code true} to wait for the collection to be loaded
     */
    public void setSync(Boolean sync) {
        this.sync = sync;
        this.async = !sync;
    }

    /**
     * Returns the timeout value for waiting the collection to be fully loaded, in milliseconds.
     *
     * @return the timeout in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout value for waiting the collection to be fully loaded, in milliseconds.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    /**
     * Returns whether to refresh the memory replicas of the collection before loading.
     *
     * @return {@code true} to refresh the memory replicas
     */
    public Boolean getRefresh() {
        return refresh;
    }

    /**
     * Sets whether to refresh the memory replicas of the collection before loading.
     *
     * @param refresh {@code true} to refresh the memory replicas
     */
    public void setRefresh(Boolean refresh) {
        this.refresh = refresh;
    }

    /**
     * Returns the list of fields to load into memory.
     *
     * @return the load fields
     */
    public List<String> getLoadFields() {
        return loadFields;
    }

    /**
     * Sets the list of fields to load into memory.
     *
     * @param loadFields the load fields
     */
    public void setLoadFields(List<String> loadFields) {
        this.loadFields = loadFields;
    }

    /**
     * Returns whether to skip loading the dynamic field into memory.
     *
     * @return {@code true} to skip loading the dynamic field
     */
    public Boolean getSkipLoadDynamicField() {
        return skipLoadDynamicField;
    }

    /**
     * Sets whether to skip loading the dynamic field into memory.
     *
     * @param skipLoadDynamicField {@code true} to skip loading the dynamic field
     */
    public void setSkipLoadDynamicField(Boolean skipLoadDynamicField) {
        this.skipLoadDynamicField = skipLoadDynamicField;
    }

    /**
     * Returns the resource groups that the collection is loaded into.
     *
     * @return the resource groups
     */
    public List<String> getResourceGroups() {
        return resourceGroups;
    }

    /**
     * Sets the resource groups that the collection is loaded into.
     *
     * @param resourceGroups the resource groups
     */
    public void setResourceGroups(List<String> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public String toString() {
        return "LoadCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", numReplicas=" + numReplicas +
                ", async=" + async +
                ", sync=" + sync +
                ", timeout=" + timeout +
                ", refresh=" + refresh +
                ", loadFields=" + loadFields +
                ", skipLoadDynamicField=" + skipLoadDynamicField +
                ", resourceGroups=" + resourceGroups +
                '}';
    }

    /**
     * Creates a new builder for {@link LoadCollectionReq}.
     *
     * @return the builder
     */
    public static LoadCollectionReqBuilder builder() {
        return new LoadCollectionReqBuilder();
    }

    public static class LoadCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Integer numReplicas = 1;
        private Boolean async = Boolean.FALSE;
        private Boolean sync = Boolean.TRUE;
        private Long timeout = 60000L;
        private Boolean refresh = Boolean.FALSE;
        private List<String> loadFields = new ArrayList<>();
        private Boolean skipLoadDynamicField = Boolean.FALSE;
        private List<String> resourceGroups = new ArrayList<>();

        private LoadCollectionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public LoadCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public LoadCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the number of replicas to load for the collection.
         *
         * @param numReplicas the number of replicas
         * @return this builder
         */
        public LoadCollectionReqBuilder numReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        /**
         * Sets whether the load operation is asynchronous.
         *
         * @param async {@code true} to run the operation asynchronously
         * @return this builder
         * @deprecated use {@link #sync(Boolean)} instead
         */
        @Deprecated
        public LoadCollectionReqBuilder async(Boolean async) {
            this.async = async;
            this.sync = !async;
            return this;
        }

        /**
         * Sets whether to wait until the collection is fully loaded.
         *
         * @param sync {@code true} to wait for the collection to be loaded
         * @return this builder
         */
        public LoadCollectionReqBuilder sync(Boolean sync) {
            this.sync = sync;
            this.async = !sync;
            return this;
        }

        /**
         * Sets the timeout value for waiting the collection to be fully loaded, in milliseconds.
         *
         * @param timeout the timeout in milliseconds
         * @return this builder
         */
        public LoadCollectionReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets whether to refresh the memory replicas of the collection before loading.
         *
         * @param refresh {@code true} to refresh the memory replicas
         * @return this builder
         */
        public LoadCollectionReqBuilder refresh(Boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        /**
         * Sets the list of fields to load into memory.
         *
         * @param loadFields the load fields
         * @return this builder
         */
        public LoadCollectionReqBuilder loadFields(List<String> loadFields) {
            this.loadFields = loadFields;
            return this;
        }

        /**
         * Sets whether to skip loading the dynamic field into memory.
         *
         * @param skipLoadDynamicField {@code true} to skip loading the dynamic field
         * @return this builder
         */
        public LoadCollectionReqBuilder skipLoadDynamicField(Boolean skipLoadDynamicField) {
            this.skipLoadDynamicField = skipLoadDynamicField;
            return this;
        }

        /**
         * Sets the resource groups that the collection is loaded into.
         *
         * @param resourceGroups the resource groups
         * @return this builder
         */
        public LoadCollectionReqBuilder resourceGroups(List<String> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        /**
         * Builds a {@link LoadCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public LoadCollectionReq build() {
            return new LoadCollectionReq(this);
        }
    }
}
