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
 * Request parameters for the {@code createPartition} API.
 */
public class CreatePartitionReq {
    private String databaseName;
    private String collectionName;
    private String partitionName;

    private CreatePartitionReq(CreatePartitionReqBuilder builder) {
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
     * Returns the name of the partition to be created.
     *
     * @return the partition name
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Sets the name of the partition to be created.
     *
     * @param partitionName the partition name
     */
    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    @Override
    public String toString() {
        return "CreatePartitionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@code CreatePartitionReq}.
     *
     * @return the builder
     */
    public static CreatePartitionReqBuilder builder() {
        return new CreatePartitionReqBuilder();
    }

    public static class CreatePartitionReqBuilder {
        private String databaseName;
        private String collectionName;
        private String partitionName;

        private CreatePartitionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public CreatePartitionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public CreatePartitionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the partition to be created.
         *
         * @param partitionName the partition name
         * @return this builder
         */
        public CreatePartitionReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Builds the {@code CreatePartitionReq}.
         *
         * @return the built request
         */
        public CreatePartitionReq build() {
            return new CreatePartitionReq(this);
        }
    }
}
