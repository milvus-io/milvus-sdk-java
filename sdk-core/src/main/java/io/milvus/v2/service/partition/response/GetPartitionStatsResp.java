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


package io.milvus.v2.service.partition.response;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class GetPartitionStatsResp {
    private Long numOfEntities;

    private GetPartitionStatsResp(Builder builder) {
        this.numOfEntities = builder.numOfEntities;
    }

    public Long getNumOfEntities() {
        return numOfEntities;
    }

    public void setNumOfEntities(Long numOfEntities) {
        this.numOfEntities = numOfEntities;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetPartitionStatsResp that = (GetPartitionStatsResp) obj;
        return new EqualsBuilder()
                .append(numOfEntities, that.numOfEntities)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return numOfEntities != null ? numOfEntities.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "GetPartitionStatsResp{" +
                "numOfEntities=" + numOfEntities +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Long numOfEntities;

        private Builder() {}

        public Builder numOfEntities(Long numOfEntities) {
            this.numOfEntities = numOfEntities;
            return this;
        }

        public GetPartitionStatsResp build() {
            return new GetPartitionStatsResp(this);
        }
    }
}
