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
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Params for load partitions RPC operation
 *
 * @author changzechuan
 */
@Getter
public class LoadPartitionsParam {
    private final String collectionName;
    private final List<String> partitionNames;
    private final boolean syncLoad;
    private final long syncLoadWaitingInterval;

    private LoadPartitionsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.syncLoad = builder.syncLoad;
        this.syncLoadWaitingInterval = builder.syncLoadWaitingInterval;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>();

        // syncLoad:
        //   Default behavior is sync loading, loadCollection() return after collection finish loading.
        private Boolean syncLoad = Boolean.TRUE;

        // syncLoadWaitingDuration:
        //   When syncLoad is ture, loadCollection() will wait until collection finish loading,
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncLoadWaitingInterval = 500L;

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

        public Builder withSyncLoad(@NonNull Boolean syncLoad) {
            this.syncLoad = syncLoad;
            return this;
        }

        public Builder withSyncLoadWaitingInterval(@NonNull Long milliseconds) {
            this.syncLoadWaitingInterval = milliseconds;
            return this;
        }

        public LoadPartitionsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (partitionNames == null || partitionNames.isEmpty()) {
                throw new ParamException("Partition names cannot be empty");
            }

            for (String name : partitionNames) {
                ParamUtils.CheckNullEmptyString(name, "Partition name");
            }

            if (syncLoad == Boolean.TRUE) {
                if (syncLoadWaitingInterval <= 0) {
                    throw new ParamException("Sync load waiting interval must be larger than zero");
                } else if (syncLoadWaitingInterval > Constant.MAX_WAITING_LOADING_INTERVAL) {
                    throw new ParamException("Sync load waiting interval must be small than "
                            + Constant.MAX_WAITING_LOADING_INTERVAL.toString() + " milliseconds");
                }
            }

            return new LoadPartitionsParam(this);
        }
    }

    @Override
    public String toString() {
        return "LoadPartitionsParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionNames.toString() + '\'' +
                ", syncLoad=" + syncLoad +
                ", syncLoadWaitingInterval=" + syncLoadWaitingInterval +
                '}';
    }
}
