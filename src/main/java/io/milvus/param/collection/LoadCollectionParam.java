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

package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

import java.util.Objects;

/**
 * Parameters for <code>loadCollection</code> interface.
 */
@Getter
public class LoadCollectionParam {
    private final String collectionName;
    private final boolean syncLoad;
    private final long syncLoadWaitingInterval;
    private final long syncLoadWaitingTimeout;
    private final int replicaNumber;

    public LoadCollectionParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.syncLoad = builder.syncLoad;
        this.syncLoadWaitingInterval = builder.syncLoadWaitingInterval;
        this.syncLoadWaitingTimeout = builder.syncLoadWaitingTimeout;
        this.replicaNumber = builder.replicaNumber;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link LoadCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;

        // syncLoad:
        //   Default behavior is sync loading, loadCollection() return after collection finish loading.
        private Boolean syncLoad = Boolean.TRUE;

        // syncLoadWaitingDuration:
        //   When syncLoad is ture, loadCollection() will wait until collection finish loading,
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncLoadWaitingInterval = 500L;

        // syncLoadWaitingTimeout:
        //   When syncLoad is ture, loadCollection() will wait until collection finish loading,
        //   this value control the waiting timeout. Unit: second. Default value: 60 seconds.
        private Long syncLoadWaitingTimeout = 60L;

        // replicaNumber:
        //   The replica number to load, default by 1
        private Integer replicaNumber = 1;

        private Builder() {
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
         * Enable sync mode for load action.
         * With sync mode enabled, the client keeps waiting until all segments of the collection successfully loaded.
         * <p>
         * If sync mode disabled, client returns at once after the loadCollection() is called.
         *
         * @param syncLoad <code>Boolean.TRUE</code> is sync mode, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withSyncLoad(@NonNull Boolean syncLoad) {
            this.syncLoad = syncLoad;
            return this;
        }

        /**
         * Sets waiting interval in sync mode. With sync mode enabled, the client will constantly check collection load state by interval.
         * Interval must be greater than zero, and cannot be larger than Constant.MAX_WAITING_LOADING_INTERVAL.
         *
         * @param milliseconds interval
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncLoadWaitingInterval(@NonNull Long milliseconds) {
            this.syncLoadWaitingInterval = milliseconds;
            return this;
        }

        /**
         * Sets timeout value for the sync mode.
         * Timeout value must be greater than zero, and cannot be greater than Constant.MAX_WAITING_LOADING_TIMEOUT.
         *
         * @param seconds time out value for sync mode
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncLoadWaitingTimeout(@NonNull Long seconds) {
            this.syncLoadWaitingTimeout = seconds;
            return this;
        }

        /**
         * Specify replica number to load
         *
         * @param replicaNumber replica number
         * @return <code>Builder</code>
         */
        public Builder withReplicaNumber(@NonNull Integer replicaNumber) {
            this.replicaNumber = replicaNumber;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link LoadCollectionParam} instance.
         *
         * @return {@link LoadCollectionParam}
         */
        public LoadCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (Objects.equals(syncLoad, Boolean.TRUE)) {
                if (syncLoadWaitingInterval <= 0) {
                    throw new ParamException("Sync load waiting interval must be larger than zero");
                } else if (syncLoadWaitingInterval > Constant.MAX_WAITING_LOADING_INTERVAL) {
                    throw new ParamException("Sync load waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_LOADING_INTERVAL.toString() + " milliseconds");
                }

                if (syncLoadWaitingTimeout <= 0) {
                    throw new ParamException("Sync load waiting timeout must be larger than zero");
                } else if (syncLoadWaitingTimeout > Constant.MAX_WAITING_LOADING_TIMEOUT) {
                    throw new ParamException("Sync load waiting timeout cannot be larger than "
                            + Constant.MAX_WAITING_LOADING_TIMEOUT.toString() + " seconds");
                }
            }

            return new LoadCollectionParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link LoadCollectionParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "LoadCollectionParam{" +
                "collectionName='" + collectionName + '\'' +
                ", syncLoad=" + syncLoad +
                ", syncLoadWaitingInterval=" + syncLoadWaitingInterval +
                '}';
    }
}
