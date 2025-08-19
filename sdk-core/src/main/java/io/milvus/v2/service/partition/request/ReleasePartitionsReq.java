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

import java.util.List;

public class ReleasePartitionsReq {
    private String collectionName;
    private List<String> partitionNames;

    private ReleasePartitionsReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReleasePartitionsReq that = (ReleasePartitionsReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = collectionName != null ? collectionName.hashCode() : 0;
        result = 31 * result + (partitionNames != null ? partitionNames.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ReleasePartitionsReq{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private List<String> partitionNames;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public ReleasePartitionsReq build() {
            return new ReleasePartitionsReq(this);
        }
    }
}
