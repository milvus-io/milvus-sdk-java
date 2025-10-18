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


package io.milvus.param.resourcegroup;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

public class TransferReplicaParam {
    private final String sourceGroupName;
    private final String targetGroupName;
    private final String collectionName;
    private final String databaseName;
    private final long replicaNumber;

    private TransferReplicaParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.replicaNumber = builder.replicaNumber;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getSourceGroupName() {
        return sourceGroupName;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public long getReplicaNumber() {
        return replicaNumber;
    }

    @Override
    public String toString() {
        return "TransferReplicaParam{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", replicaNumber=" + replicaNumber +
                '}';
    }

    /**
     * Builder for {@link TransferReplicaParam} class.
     */
    public static final class Builder {
        private String sourceGroupName;
        private String targetGroupName;
        private String collectionName;
        private String databaseName;
        private Long replicaNumber = 0L;

        private Builder() {
        }

        /**
         * Sets the source group name. group name cannot be empty or null.
         *
         * @param groupName source group name
         * @return <code>Builder</code>
         */
        public Builder withSourceGroupName(String groupName) {
            if (groupName == null) {
                throw new IllegalArgumentException("Source group name cannot be null");
            }
            this.sourceGroupName = groupName;
            return this;
        }

        /**
         * Sets the target group name. group name cannot be empty or null.
         *
         * @param groupName target group name
         * @return <code>Builder</code>
         */
        public Builder withTargetGroupName(String groupName) {
            if (groupName == null) {
                throw new IllegalArgumentException("Target group name cannot be null");
            }
            this.targetGroupName = groupName;
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Specify number of replicas to transfer
         *
         * @param replicaNumber number of replicas to transfer
         * @return <code>Builder</code>
         */
        public Builder withReplicaNumber(Long replicaNumber) {
            if (replicaNumber == null) {
                throw new IllegalArgumentException("Replica number cannot be null");
            }
            this.replicaNumber = replicaNumber;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link TransferReplicaParam} instance.
         *
         * @return {@link TransferReplicaParam}
         */
        public TransferReplicaParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(sourceGroupName, "Source group name");
            ParamUtils.CheckNullEmptyString(targetGroupName, "Target group name");
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (replicaNumber <= 0) {
                throw new ParamException("Replica number must be specified and greater than zero");
            }

            return new TransferReplicaParam(this);
        }
    }

}
