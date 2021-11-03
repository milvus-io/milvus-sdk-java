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

import javax.annotation.Nonnull;

/**
 * Params for load partitions RPC operation
 *
 * @author changzechuan
 */
public class LoadPartitionsParam {
    private final String collectionName;
    private final String[] partitionNames;

    private LoadPartitionsParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String[] getPartitionNames() {
        return partitionNames;
    }

    public static final class Builder {
        private String collectionName;
        private String[] partitionNames;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withPartitionNames(@Nonnull String[] partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public LoadPartitionsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (partitionNames == null || partitionNames.length == 0) {
                throw new ParamException("Partition names cannot be empty");
            }

            for (String name : partitionNames) {
                ParamUtils.CheckNullEmptyString(name, "Partition name");
            }

            return new LoadPartitionsParam(this);
        }
    }
}
