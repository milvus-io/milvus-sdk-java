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

public class GetPartitionStatsReq {
    private String databaseName;
    private String collectionName;
    private String partitionName;

    private GetPartitionStatsReq(GetPartitionStatsReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
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

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    @Override
    public String toString() {
        return "GetPartitionStatsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }

    public static GetPartitionStatsReqBuilder builder() {
        return new GetPartitionStatsReqBuilder();
    }

    public static class GetPartitionStatsReqBuilder {
        private String databaseName;
        private String collectionName;
        private String partitionName;

        private GetPartitionStatsReqBuilder() {
        }

        public GetPartitionStatsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public GetPartitionStatsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetPartitionStatsReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public GetPartitionStatsReq build() {
            return new GetPartitionStatsReq(this);
        }
    }
}
