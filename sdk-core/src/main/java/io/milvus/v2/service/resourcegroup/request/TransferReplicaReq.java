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

    public static TransferReplicaReqBuilder builder() {
        return new TransferReplicaReqBuilder();
    }

    public String getSourceGroupName() {
        return sourceGroupName;
    }

    public void setSourceGroupName(String sourceGroupName) {
        this.sourceGroupName = sourceGroupName;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public void setTargetGroupName(String targetGroupName) {
        this.targetGroupName = targetGroupName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getNumberOfReplicas() {
        return numberOfReplicas;
    }

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

        public TransferReplicaReqBuilder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        public TransferReplicaReqBuilder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        public TransferReplicaReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public TransferReplicaReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public TransferReplicaReqBuilder numberOfReplicas(Long numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        public TransferReplicaReq build() {
            return new TransferReplicaReq(this);
        }
    }
}
