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

import java.util.List;

/**
 * Request parameters for the {@code releasePartitions} API.
 */
public class ReleasePartitionsReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;

    private ReleasePartitionsReq(ReleasePartitionsReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the names of the partitions to be released.
     *
     * @return the list of partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the names of the partitions to be released.
     *
     * @param partitionNames the list of partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    @Override
    public String toString() {
        return "ReleasePartitionsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                '}';
    }

    /**
     * Creates a new builder for {@code ReleasePartitionsReq}.
     *
     * @return the builder
     */
    public static ReleasePartitionsReqBuilder builder() {
        return new ReleasePartitionsReqBuilder();
    }

    public static class ReleasePartitionsReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames;

        private ReleasePartitionsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public ReleasePartitionsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public ReleasePartitionsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the names of the partitions to be released.
         *
         * @param partitionNames the list of partition names
         * @return this builder
         */
        public ReleasePartitionsReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Builds the {@code ReleasePartitionsReq}.
         *
         * @return the built request
         */
        public ReleasePartitionsReq build() {
            return new ReleasePartitionsReq(this);
        }
    }
}
