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

package io.milvus.v2.service.resourcegroup.request;

/**
 * Request parameters for the {@code transferReplica} API.
 */
public class TransferReplicaReq {
    private String sourceGroupName;
    private String targetGroupName;
    private String collectionName;
    private String databaseName;
    private Long numberOfReplicas;

    private TransferReplicaReq(TransferReplicaReqBuilder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.numberOfReplicas = builder.numberOfReplicas;
    }

    /**
     * Creates a new builder for {@code TransferReplicaReq}.
     *
     * @return the builder
     */
    public static TransferReplicaReqBuilder builder() {
        return new TransferReplicaReqBuilder();
    }

    /**
     * Returns the name of the source resource group.
     *
     * @return the source resource group name
     */
    public String getSourceGroupName() {
        return sourceGroupName;
    }

    /**
     * Sets the name of the source resource group.
     *
     * @param sourceGroupName the source resource group name
     */
    public void setSourceGroupName(String sourceGroupName) {
        this.sourceGroupName = sourceGroupName;
    }

    /**
     * Returns the name of the target resource group.
     *
     * @return the target resource group name
     */
    public String getTargetGroupName() {
        return targetGroupName;
    }

    /**
     * Sets the name of the target resource group.
     *
     * @param targetGroupName the target resource group name
     */
    public void setTargetGroupName(String targetGroupName) {
        this.targetGroupName = targetGroupName;
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
     * Returns the number of replicas to be transferred.
     *
     * @return the number of replicas
     */
    public Long getNumberOfReplicas() {
        return numberOfReplicas;
    }

    /**
     * Sets the number of replicas to be transferred.
     *
     * @param numberOfReplicas the number of replicas
     */
    public void setNumberOfReplicas(Long numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    @Override
    public String toString() {
        return "TransferReplicaReq{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", numberOfReplicas=" + numberOfReplicas +
                '}';
    }

    public static class TransferReplicaReqBuilder {
        private String sourceGroupName;
        private String targetGroupName;
        private String collectionName;
        private String databaseName;
        private Long numberOfReplicas = 1L;

        /**
         * Sets the name of the source resource group.
         *
         * @param sourceGroupName the source resource group name
         * @return this builder
         */
        public TransferReplicaReqBuilder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        /**
         * Sets the name of the target resource group.
         *
         * @param targetGroupName the target resource group name
         * @return this builder
         */
        public TransferReplicaReqBuilder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public TransferReplicaReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public TransferReplicaReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the number of replicas to be transferred.
         *
         * @param numberOfReplicas the number of replicas
         * @return this builder
         */
        public TransferReplicaReqBuilder numberOfReplicas(Long numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        /**
         * Builds the {@code TransferReplicaReq}.
         *
         * @return the built request
         */
        public TransferReplicaReq build() {
            return new TransferReplicaReq(this);
        }
    }
}
