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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Parameters for <code>flush</code> interface.
 * Note that the flush interface is not exposed currently.
 */
public class FlushParam {
    private final String databaseName;
    private final List<String> collectionNames;
    private final Boolean syncFlush;
    private final long syncFlushWaitingInterval;
    private final long syncFlushWaitingTimeout;

    private FlushParam(Builder builder) {
        if (builder.collectionNames == null) {
            throw new IllegalArgumentException("collectionNames cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionNames = builder.collectionNames;
        this.syncFlush = builder.syncFlush;
        this.syncFlushWaitingInterval = builder.syncFlushWaitingInterval;
        this.syncFlushWaitingTimeout = builder.syncFlushWaitingTimeout;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getCollectionNames() {
        return collectionNames;
    }

    public Boolean getSyncFlush() {
        return syncFlush;
    }

    public long getSyncFlushWaitingInterval() {
        return syncFlushWaitingInterval;
    }

    public long getSyncFlushWaitingTimeout() {
        return syncFlushWaitingTimeout;
    }

    @Override
    public String toString() {
        return "FlushParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionNames=" + collectionNames +
                ", syncFlush=" + syncFlush +
                ", syncFlushWaitingInterval=" + syncFlushWaitingInterval +
                ", syncFlushWaitingTimeout=" + syncFlushWaitingTimeout +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link FlushParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private final List<String> collectionNames = new ArrayList<>();

        // syncFlush:
        //   Default behavior is sync flushing, flush() return after collection finish flushing.
        private Boolean syncFlush = Boolean.TRUE;

        // syncFlushWaitingInterval:
        //   When syncFlush is ture, flush() will wait until collection finish flushing,
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncFlushWaitingInterval = 500L;

        // syncFlushWaitingTimeout:
        //   When syncFlush is ture, flush() will wait until collection finish flushing,
        //   this value control the waiting timeout. Unit: second. Default value: 60 seconds.
        private Long syncFlushWaitingTimeout = 60L;

        private Builder() {
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
         * Sets a list of collections to be flushed.
         *
         * @param collectionNames a list of collections
         * @return <code>Builder</code>
         */
        public Builder withCollectionNames(List<String> collectionNames) {
            if (collectionNames == null) {
                throw new IllegalArgumentException("collectionNames cannot be null");
            }
            this.collectionNames.addAll(collectionNames);
            return this;
        }

        /**
         * Adds a collection to be flushed.
         *
         * @param collectionName name of the collections
         * @return <code>Builder</code>
         */
        public Builder addCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionNames.add(collectionName);
            return this;
        }

        /**
         * Sets the flush function to sync mode.
         * With sync mode enabled, the client keeps waiting until all segments of the collection successfully flushed.
         * <p>
         * If sync mode disabled, client returns at once after the flush() is called.
         *
         * @param syncFlush <code>Boolean.TRUE</code> is sync mode, <code>Boolean.FALSE</code> is not
         * @return <code>Builder</code>
         */
        public Builder withSyncFlush(Boolean syncFlush) {
            if (syncFlush == null) {
                throw new IllegalArgumentException("syncFlush cannot be null");
            }
            this.syncFlush = syncFlush;
            return this;
        }

        /**
         * Sets waiting interval in sync mode. With sync mode enabled, the client will constantly check segments state by interval.
         * Interval must be greater than zero, and cannot be greater than Constant.MAX_WAITING_FLUSHING_INTERVAL.
         *
         * @param milliseconds interval
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncFlushWaitingInterval(Long milliseconds) {
            if (milliseconds == null) {
                throw new IllegalArgumentException("milliseconds cannot be null");
            }
            this.syncFlushWaitingInterval = milliseconds;
            return this;
        }

        /**
         * Sets timeout value for sync mode.
         * Timeout value must be greater than zero, and cannot be greater than Constant.MAX_WAITING_FLUSHING_TIMEOUT.
         *
         * @param seconds time out value for sync mode
         * @return <code>Builder</code>
         * @see Constant
         */
        public Builder withSyncFlushWaitingTimeout(Long seconds) {
            if (seconds == null) {
                throw new IllegalArgumentException("seconds cannot be null");
            }
            this.syncFlushWaitingTimeout = seconds;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link FlushParam} instance.
         *
         * @return {@link FlushParam}
         */
        public FlushParam build() throws ParamException {
            if (collectionNames.isEmpty()) {
                throw new ParamException("CollectionNames can not be empty");
            }

            for (String name : collectionNames) {
                ParamUtils.CheckNullEmptyString(name, "Collection name");
            }

            if (Objects.equals(syncFlush, Boolean.TRUE)) {
                if (syncFlushWaitingInterval <= 0) {
                    throw new ParamException("Sync flush waiting interval must be larger than zero");
                } else if (syncFlushWaitingInterval > Constant.MAX_WAITING_FLUSHING_INTERVAL) {
                    throw new ParamException("Sync flush waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_FLUSHING_INTERVAL.toString() + " milliseconds");
                }

                if (syncFlushWaitingTimeout <= 0) {
                    throw new ParamException("Sync flush waiting timeout must be larger than zero");
                } else if (syncFlushWaitingTimeout > Constant.MAX_WAITING_FLUSHING_TIMEOUT) {
                    throw new ParamException("Sync flush waiting timeout cannot be larger than "
                            + Constant.MAX_WAITING_FLUSHING_TIMEOUT.toString() + " seconds");
                }
            }

            return new FlushParam(this);
        }
    }

}
