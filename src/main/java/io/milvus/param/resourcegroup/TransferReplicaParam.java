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
import lombok.Getter;
import lombok.NonNull;

@Getter
public class TransferReplicaParam {
    private final String sourceGroupName;
    private final String targetGroupName;
    private final String collectionName;
    private final String databaseName;
    private final long replicaNumber;

    private TransferReplicaParam(@NonNull Builder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.replicaNumber = builder.replicaNumber;
    }

    public static Builder newBuilder() {
        return new Builder();
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
        public Builder withSourceGroupName(@NonNull String groupName) {
            this.sourceGroupName = groupName;
            return this;
        }

        /**
         * Sets the target group name. group name cannot be empty or null.
         *
         * @param groupName target group name
         * @return <code>Builder</code>
         */
        public Builder withTargetGroupName(@NonNull String groupName) {
            this.targetGroupName = groupName;
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
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
        public Builder withReplicaNumber(@NonNull Long replicaNumber) {
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

    /**
     * Constructs a <code>String</code> by {@link TransferReplicaParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "TransferReplicaParam{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                "targetGroupName='" + targetGroupName + '\'' +
                "collectionName='" + collectionName + '\'' +
                "databaseName='" + databaseName + '\'' +
                "replicaNumber='" + replicaNumber +
                '}';
    }
}
