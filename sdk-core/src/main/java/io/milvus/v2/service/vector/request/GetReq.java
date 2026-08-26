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

import java.util.List;

/**
 * Request parameters for the {@code get} API.
 */
public class GetReq {
    private String databaseName;
    private String collectionName;
    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    private String clusterId;
    private String partitionName = "";
    private List<String> partitionNames;
    private List<Object> ids;
    private List<String> outputFields;

    private GetReq(GetReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionName = builder.partitionName;
        this.partitionNames = builder.partitionNames;
        this.ids = builder.ids;
        this.outputFields = builder.outputFields;
    }

    /**
     * Creates a new {@code GetReq} builder.
     *
     * @return the builder
     */
    public static GetReqBuilder builder() {
        return new GetReqBuilder();
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
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    public String getClusterId() {
        return clusterId;
    }

    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Returns the partition name to get from.
     *
     * @return the partition name
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Sets the partition name to get from.
     *
     * @param partitionName the partition name
     */
    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    /**
     * Returns the partition names to get from.
     *
     * @return the partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the partition names to get from.
     *
     * @param partitionNames the partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    /**
     * Returns the primary key values of the entities to get.
     *
     * @return the primary key values
     */
    public List<Object> getIds() {
        return ids;
    }

    /**
     * Sets the primary key values of the entities to get.
     *
     * @param ids the primary key values
     */
    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    /**
     * Returns the fields to return for each retrieved entity.
     *
     * @return the output fields
     */
    public List<String> getOutputFields() {
        return outputFields;
    }

    /**
     * Sets the fields to return for each retrieved entity.
     *
     * @param outputFields the output fields
     */
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public String toString() {
        return "GetReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", ids=" + ids +
                ", outputFields=" + outputFields +
                '}';
    }

    public static class GetReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private String partitionName = "";
        private List<String> partitionNames;
        private List<Object> ids;
        private List<String> outputFields;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public GetReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public GetReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public GetReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition name to get from.
         *
         * @param partitionName the partition name
         * @return this builder
         */
        public GetReqBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the partition names to get from.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public GetReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the primary key values of the entities to get.
         *
         * @param ids the primary key values
         * @return this builder
         */
        public GetReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        /**
         * Sets the fields to return for each retrieved entity.
         *
         * @param outputFields the output fields
         * @return this builder
         */
        public GetReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Builds the {@link GetReq}.
         *
         * @return the request
         */
        public GetReq build() {
            return new GetReq(this);
        }
    }
}
