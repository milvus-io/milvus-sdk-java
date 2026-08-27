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

/**
 * Request parameters for the {@code dropPartition} API.
 */
public class DropPartitionReq {
    private String databaseName;
    private String collectionName;
    private String partitionName;

    private DropPartitionReq(DropPartitionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
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
     * Returns the name of the partition to be dropped.
     *
     * @return the partition name
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Sets the name of the partition to be dropped.
     *
     * @param partitionName the partition name
     */
    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    @Override
    public String toString() {
        return "DropPartitionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@code DropPartitionReq}.
     *
     * @return the builder
     */
    public static DropPartitionReqBuilder builder() {
        return new DropPartitionReqBuilder();
    }

    public static class DropPartitionReqBuilder {
        private String databaseName;
        private String collectionName;
        private String partitionName;

        private DropPartitionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropPartitionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropPartitionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the partition to be dropped.
         *
         * @param partitionName the partition name
         * @return this builder
         */
        public DropPartitionReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Builds the {@code DropPartitionReq}.
         *
         * @return the built request
         */
        public DropPartitionReq build() {
            return new DropPartitionReq(this);
        }
    }
}
