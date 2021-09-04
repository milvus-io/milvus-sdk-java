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

/**
 * Params for get partition statistics RPC operation
 *
 * @author changzechuan
 */
public class GetPartitionStatisticsParam {
    private final String collectionName;
    private final String partitionName;

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public GetPartitionStatisticsParam(Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
    }

    public static final class Builder {
        private String collectionName;
        private String partitionName;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withPartitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public GetPartitionStatisticsParam build() {
            return new GetPartitionStatisticsParam(this);
        }
    }
}
