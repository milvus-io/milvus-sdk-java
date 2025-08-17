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

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

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

    private LoadPartitionsReq(Builder builder) {
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

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public Integer getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(Integer numReplicas) {
        this.numReplicas = numReplicas;
    }

    public Boolean getSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Boolean getRefresh() {
        return refresh;
    }

    public void setRefresh(Boolean refresh) {
        this.refresh = refresh;
    }

    public List<String> getLoadFields() {
        return loadFields;
    }

    public void setLoadFields(List<String> loadFields) {
        this.loadFields = loadFields;
    }

    public Boolean getSkipLoadDynamicField() {
        return skipLoadDynamicField;
    }

    public void setSkipLoadDynamicField(Boolean skipLoadDynamicField) {
        this.skipLoadDynamicField = skipLoadDynamicField;
    }

    public List<String> getResourceGroups() {
        return resourceGroups;
    }

    public void setResourceGroups(List<String> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        LoadPartitionsReq that = (LoadPartitionsReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .append(numReplicas, that.numReplicas)
                .append(sync, that.sync)
                .append(timeout, that.timeout)
                .append(refresh, that.refresh)
                .append(loadFields, that.loadFields)
                .append(skipLoadDynamicField, that.skipLoadDynamicField)
                .append(resourceGroups, that.resourceGroups)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = collectionName != null ? collectionName.hashCode() : 0;
        result = 31 * result + (partitionNames != null ? partitionNames.hashCode() : 0);
        result = 31 * result + (numReplicas != null ? numReplicas.hashCode() : 0);
        result = 31 * result + (sync != null ? sync.hashCode() : 0);
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (refresh != null ? refresh.hashCode() : 0);
        result = 31 * result + (loadFields != null ? loadFields.hashCode() : 0);
        result = 31 * result + (skipLoadDynamicField != null ? skipLoadDynamicField.hashCode() : 0);
        result = 31 * result + (resourceGroups != null ? resourceGroups.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LoadPartitionsReq{" +
                "collectionName='" + collectionName + '\'' +
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();
        private Integer numReplicas = 1;
        private Boolean sync = Boolean.TRUE; // wait the partitions to be fully loaded
        private Long timeout = 60000L; // timeout value for waiting the partitions to be fully loaded
        private Boolean refresh = Boolean.FALSE;
        private List<String> loadFields = new ArrayList<>();
        private Boolean skipLoadDynamicField = Boolean.FALSE;
        private List<String> resourceGroups = new ArrayList<>();

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder numReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        public Builder sync(Boolean sync) {
            this.sync = sync;
            return this;
        }

        public Builder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder refresh(Boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        public Builder loadFields(List<String> loadFields) {
            this.loadFields = loadFields;
            return this;
        }

        public Builder skipLoadDynamicField(Boolean skipLoadDynamicField) {
            this.skipLoadDynamicField = skipLoadDynamicField;
            return this;
        }

        public Builder resourceGroups(List<String> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        public LoadPartitionsReq build() {
            return new LoadPartitionsReq(this);
        }
    }
}
