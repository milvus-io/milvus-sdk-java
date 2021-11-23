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

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>releasePartition</code> interface.
 */
@Getter
public class ReleasePartitionsParam {
    private final String collectionName;
    private final List<String> partitionNames;

    private ReleasePartitionsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>ReleasePartitionsParam</code> class.
     */
    public static final class Builder {
        private String collectionName;
        private final List<String> partitionNames = new ArrayList<>();

        private Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Set partition names list. Partition names list cannot be null or empty.
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * Add a partition name. Partition name cannot be empty or null.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(@NonNull String partitionName) {
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
        }

        /**
         * Verify parameters and create a new <code>ReleasePartitionsParam</code> instance.
         *
         * @return <code>ReleasePartitionsParam</code>
         */
        public ReleasePartitionsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (partitionNames.isEmpty()) {
                throw new ParamException("Partition names cannot be empty");
            }

            for (String name : partitionNames) {
                ParamUtils.CheckNullEmptyString(name, "Partition name");
            }

            return new ReleasePartitionsParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>ReleasePartitionsParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ReleasePartitionsParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames='" + partitionNames.toString() + '\'' +
                '}';
    }
}
