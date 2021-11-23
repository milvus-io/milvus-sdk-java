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
import io.milvus.grpc.ShowType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>showPartition</code> interface.
 */
@Getter
public class ShowPartitionsParam {
    private final String collectionName;
    private final List<String> partitionNames;
    private final ShowType showType;

    private ShowPartitionsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.showType = builder.showType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>ShowPartitionsParam</code> class.
     */
    public static final class Builder {
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();

        // showType:
        //   default showType = ShowType.All
        //   if partitionNames is not empty, set showType = ShowType.InMemory
        private ShowType showType = ShowType.All;

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
         * Verify parameters and create a new <code>ShowPartitionsParam</code> instance.
         *
         * @return <code>ShowPartitionsParam</code>
         */
        public ShowPartitionsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (partitionNames != null && !partitionNames.isEmpty()) {
                for (String partitionName : partitionNames) {
                    ParamUtils.CheckNullEmptyString(partitionName, "Partition name");
                }
                this.showType = ShowType.InMemory;
            }

            return new ShowPartitionsParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>ShowPartitionsParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ShowPartitionsParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames='" + partitionNames.toString() + '\'' +
                ", showType=" + showType.toString() +
                '}';
    }
}
