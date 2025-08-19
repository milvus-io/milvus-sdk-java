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

public class HasPartitionReq {
    private String collectionName;
    private String partitionName;

    private HasPartitionReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HasPartitionReq that = (HasPartitionReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(partitionName, that.partitionName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = collectionName != null ? collectionName.hashCode() : 0;
        result = 31 * result + (partitionName != null ? partitionName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HasPartitionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private String partitionName;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public HasPartitionReq build() {
            return new HasPartitionReq(this);
        }
    }
}
