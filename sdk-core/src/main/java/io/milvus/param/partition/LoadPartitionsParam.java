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

package io.milvus.param.partition;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Parameters for <code>loadPartition</code> interface.
 */
public class LoadPartitionsParam {
    private final String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final boolean syncLoad;
    private final long syncLoadWaitingInterval;
    private final long syncLoadWaitingTimeout;
    private final int replicaNumber;
    private final boolean refresh;
    private final List<String> resourceGroups;
    private final List<String> loadFields;
    private final boolean skipLoadDynamicField;

    private LoadPartitionsParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.syncLoad = builder.syncLoad;
        this.syncLoadWaitingInterval = builder.syncLoadWaitingInterval;
        this.syncLoadWaitingTimeout = builder.syncLoadWaitingTimeout;
        this.replicaNumber = builder.replicaNumber;
        this.refresh = builder.refresh;
        this.resourceGroups = builder.resourceGroups;
        this.loadFields = builder.loadFields;
        this.skipLoadDynamicField = builder.skipLoadDynamicField;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods
    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isSyncLoad() {
        return syncLoad;
    }

    public long getSyncLoadWaitingInterval() {
        return syncLoadWaitingInterval;
    }

    public long getSyncLoadWaitingTimeout() {
        return syncLoadWaitingTimeout;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public boolean isRefresh() {
        return refresh;
    }

    public List<String> getResourceGroups() {
        return resourceGroups;
    }

    public List<String> getLoadFields() {
        return loadFields;
    }

    public boolean isSkipLoadDynamicField() {
        return skipLoadDynamicField;
    }

    // toString method
    @Override
    public String toString() {
        return "LoadPartitionsParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", syncLoad=" + syncLoad +
                ", syncLoadWaitingInterval=" + syncLoadWaitingInterval +
                ", syncLoadWaitingTimeout=" + syncLoadWaitingTimeout +
                ", replicaNumber=" + replicaNumber +
                ", refresh=" + refresh +
                ", resourceGroups=" + resourceGroups +
                ", loadFields=" + loadFields +
                ", skipLoadDynamicField=" + skipLoadDynamicField +
                '}';
    }

    /**
     * Builder for {@link LoadPartitionsParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;
        private final List<String> partitionNames = new ArrayList<>();

        // syncLoad:
        //   Default behavior is sync loading, loadPartition() return after partition finish loading.
        private Boolean syncLoad = Boolean.TRUE;

        // syncLoadWaitingDuration:
        //   When syncLoad is ture, loadPartition() will wait until partition finish loading,
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncLoadWaitingInterval = 500L;

        // syncLoadWaitingTimeout:
        //   When syncLoad is ture, loadPartition() will wait until partition finish loading,
        //   this value control the waiting timeout. Unit: second. Default value: 60 seconds.
        private Long syncLoadWaitingTimeout = 60L;

        // replicaNumber:
        //   The replica number to load
        private Integer replicaNumber = 0;

        // refresh:
        //   This flag must be set to FALSE when first time call the loadPartitions().
        //   After loading a partition, call loadPartitions() again with refresh=TRUE,
        //   the server will look for new segments that are not loaded yet and tries to load them up.
        private Boolean refresh = Boolean.FALSE;

        // resourceGroups:
        //   Specify the target resource groups to load the replicas.
        //   If not specified, the replicas will be loaded into the default resource group.
        private List<String> resourceGroups = new ArrayList<>();

        // loadFields:
        //   Specify load fields list needed during this load.
        //   If not specified, all the fields will be loaded.
        private List<String> loadFields = new ArrayList<>();

        // skipLoadDynamicField:
        //   Specify whether this load shall skip dynamic schema field.
        private Boolean skipLoadDynamicField = Boolean.FALSE;

        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition names list. Partition names list cannot be null or empty.
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(List<String> partitionNames) {
            if (partitionNames == null) {
                throw new IllegalArgumentException("Partition names cannot be null");
            }
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * Adds a partition by name. Partition name cannot be empty or null.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(String partitionName) {
            if (partitionName == null) {
                throw new IllegalArgumentException("Partition name cannot be null");
            }
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
        }

        /**
         * Enables sync mode for load action.
         * With sync mode enabled, the client keeps waiting until all segments of the partition are successfully loaded.
         * <p>
         * Without sync mode disabled, client returns at once after the loadPartitions() is called.
         *
         * @param syncLoad <code>Boolean.TRUE</code> is sync mode, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withSyncLoad(Boolean syncLoad) {
            if (syncLoad == null) {
                throw new IllegalArgumentException("SyncLoad cannot be null");
            }
            this.syncLoad = syncLoad;
            return this;
        }

        /**
         * Sets the waiting interval for sync mode. In sync mode, the client constantly checks partition load state by interval.
         * Interval must be greater than zero, and cannot be greater than Constant.MAX_WAITING_LOADING_INTERVAL.
         *
         * @param milliseconds interval
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncLoadWaitingInterval(Long milliseconds) {
            if (milliseconds == null) {
                throw new IllegalArgumentException("Milliseconds cannot be null");
            }
            this.syncLoadWaitingInterval = milliseconds;
            return this;
        }

        /**
         * Sets the timeout value for sync mode.
         * Timeout value must be greater than zero, and cannot be greater than Constant.MAX_WAITING_LOADING_TIMEOUT.
         *
         * @param seconds time out value for sync mode
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncLoadWaitingTimeout(Long seconds) {
            if (seconds == null) {
                throw new IllegalArgumentException("Seconds cannot be null");
            }
            this.syncLoadWaitingTimeout = seconds;
            return this;
        }

        /**
         * Specify replica number to load
         *
         * @param replicaNumber replica number
         * @return <code>Builder</code>
         */
        public Builder withReplicaNumber(Integer replicaNumber) {
            if (replicaNumber == null) {
                throw new IllegalArgumentException("Replica number cannot be null");
            }
            this.replicaNumber = replicaNumber;
            return this;
        }

        /**
         * Whether to enable refresh mode.
         * Refresh mode renews the segment list of this collection before loading.
         * This flag must be set to FALSE when first time call the loadPartitions().
         * After loading a collection, call loadPartitions() again with refresh=TRUE,
         * the server will look for new segments that are not loaded yet and tries to load them up.
         *
         * @param refresh <code>Boolean.TRUE</code> is refresh mode, <code>Boolean.FALSE</code> is not
         * @return <code>Builder</code>
         */
        public Builder withRefresh(Boolean refresh) {
            if (refresh == null) {
                throw new IllegalArgumentException("Refresh cannot be null");
            }
            this.refresh = refresh;
            return this;
        }

        /**
         * Specify the target resource groups to load the replicas.
         * If not specified, the replicas will be loaded into the default resource group.
         *
         * @param resourceGroups a <code>List</code> of {@link String}
         * @return <code>Builder</code>
         */
        public Builder withResourceGroups(List<String> resourceGroups) {
            if (resourceGroups == null) {
                throw new IllegalArgumentException("Resource groups cannot be null");
            }
            this.resourceGroups.addAll(resourceGroups);
            return this;
        }

        /**
         * Specify load fields list needed during this load.
         * If not specified, all the fields will be loaded.
         *
         * @param loadFields a <code>List</code> of {@link String}
         * @return <code>Builder</code>
         */
        public Builder withLoadFields(List<String> loadFields) {
            if (loadFields == null) {
                throw new IllegalArgumentException("Load fields cannot be null");
            }
            loadFields.forEach((field) -> {
                if (!this.loadFields.contains(field)) {
                    this.loadFields.add(field);
                }
            });
            return this;
        }

        /**
         * Specify load fields list needed during this load. If not specified, all the fields will be loaded.
         * Default is False.
         *
         * @param skip <code>Boolean.TRUE</code> skip dynamic field, <code>Boolean.FALSE</code> is not
         * @return <code>Builder</code>
         */
        public Builder withSkipLoadDynamicField(Boolean skip) {
            if (skip == null) {
                throw new IllegalArgumentException("Skip cannot be null");
            }
            this.skipLoadDynamicField = skip;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link LoadPartitionsParam} instance.
         *
         * @return {@link LoadPartitionsParam}
         */
        public LoadPartitionsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (partitionNames.isEmpty()) {
                throw new ParamException("Partition names cannot be empty");
            }

            for (String name : partitionNames) {
                ParamUtils.CheckNullEmptyString(name, "Partition name");
            }

            if (Objects.equals(syncLoad, Boolean.TRUE)) {
                if (syncLoadWaitingInterval <= 0) {
                    throw new ParamException("Sync load waiting interval must be larger than zero");
                } else if (syncLoadWaitingInterval > Constant.MAX_WAITING_LOADING_INTERVAL) {
                    throw new ParamException("Sync load waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_LOADING_INTERVAL.toString() + " milliseconds");
                }

                if (syncLoadWaitingTimeout <= 0) {
                    throw new ParamException("Sync load waiting interval must be larger than zero");
                } else if (syncLoadWaitingTimeout > Constant.MAX_WAITING_LOADING_TIMEOUT) {
                    throw new ParamException("Sync load waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_LOADING_TIMEOUT.toString() + " seconds");
                }
            }

            return new LoadPartitionsParam(this);
        }
    }

}
