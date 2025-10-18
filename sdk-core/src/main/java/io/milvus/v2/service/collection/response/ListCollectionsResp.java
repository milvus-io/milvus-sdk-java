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

package io.milvus.v2.service.collection.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

import io.milvus.v2.service.collection.CollectionInfo;

public class ListCollectionsResp {
    private List<String> collectionNames;
    private List<CollectionInfo> collectionInfos;

    private ListCollectionsResp(Builder builder) {
        this.collectionNames = builder.collectionNames != null ? builder.collectionNames : new ArrayList<>();
        this.collectionInfos = builder.collectionInfos != null ? builder.collectionInfos : new ArrayList<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public List<String> getCollectionNames() {
        return collectionNames;
    }

    public List<CollectionInfo> getCollectionInfos() {
        return collectionInfos;
    }

    // Setters
    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    public void setCollectionInfos(List<CollectionInfo> collectionInfos) {
        this.collectionInfos = collectionInfos;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ListCollectionsResp that = (ListCollectionsResp) obj;

        return new EqualsBuilder()
                .append(collectionNames, that.collectionNames)
                .append(collectionInfos, that.collectionInfos)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionNames)
                .append(collectionInfos)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "ListCollectionsResp{" +
                "collectionNames=" + collectionNames +
                ", collectionInfos=" + collectionInfos +
                '}';
    }

    public static class Builder {
        private List<String> collectionNames;
        private List<CollectionInfo> collectionInfos;

        public Builder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public Builder collectionInfos(List<CollectionInfo> collectionInfos) {
            this.collectionInfos = collectionInfos;
            return this;
        }

        public ListCollectionsResp build() {
            return new ListCollectionsResp(this);
        }
    }
}
