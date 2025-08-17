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

package io.milvus.v2.service.vector.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class GetReq {
    private String collectionName;
    private String partitionName = "";
    private List<Object> ids;
    private List<String> outputFields;

    private GetReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.ids = builder.ids;
        this.outputFields = builder.outputFields;
    }

    public static Builder builder() {
        return new Builder();
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

    public List<Object> getIds() {
        return ids;
    }

    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetReq that = (GetReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(partitionName, that.partitionName)
                .append(ids, that.ids)
                .append(outputFields, that.outputFields)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(partitionName)
                .append(ids)
                .append(outputFields)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "GetReq{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", ids=" + ids +
                ", outputFields=" + outputFields +
                '}';
    }

    public static class Builder {
        private String collectionName;
        private String partitionName = "";
        private List<Object> ids;
        private List<String> outputFields;

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        public Builder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public GetReq build() {
            return new GetReq(this);
        }
    }
}
