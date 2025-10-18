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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

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

    private LoadCollectionReq(Builder builder) {
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

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Integer getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(Integer numReplicas) {
        this.numReplicas = numReplicas;
    }

    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
        this.sync = !async;
    }

    public Boolean getSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
        this.async = !sync;
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
        LoadCollectionReq that = (LoadCollectionReq) obj;
        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(numReplicas, that.numReplicas)
                .append(async, that.async)
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
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionName)
                .append(numReplicas)
                .append(async)
                .append(sync)
                .append(timeout)
                .append(refresh)
                .append(loadFields)
                .append(skipLoadDynamicField)
                .append(resourceGroups)
                .toHashCode();
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
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

        private Builder() {}

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder numReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        @Deprecated
        public Builder async(Boolean async) {
            this.async = async;
            this.sync = !async;
            return this;
        }

        public Builder sync(Boolean sync) {
            this.sync = sync;
            this.async = !sync;
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

        public LoadCollectionReq build() {
            return new LoadCollectionReq(this);
        }
    }
}
