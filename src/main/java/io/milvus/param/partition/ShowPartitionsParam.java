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
 * Params for show partition RPC operation
 *
 * @author changzechuan
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

    public static final class Builder {
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();

        // showType:
        //   default showType = ShowType.All
        //   if partitionNames is not empty, set showType = ShowType.InMemory
        private ShowType showType = ShowType.All;

        private Builder() {
        }

        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder addPartitionName(@NonNull String partitionName) {
            this.partitionNames.add(partitionName);
            return this;
        }

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

    @Override
    public String toString() {
        return "ShowPartitionsParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames='" + partitionNames.toString() + '\'' +
                ", showType=" + showType.toString() +
                '}';
    }
}
