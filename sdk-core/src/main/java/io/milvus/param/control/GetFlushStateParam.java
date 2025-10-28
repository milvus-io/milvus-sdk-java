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

package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>getMetric</code> interface.
 */
public class GetFlushStateParam {
    private final String databaseName;
    private final String collectionName;
    private final List<Long> segmentIDs;
    private final Long flushTs;

    private GetFlushStateParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.segmentIDs = builder.segmentIDs;
        this.flushTs = builder.flushTs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<Long> getSegmentIDs() {
        return segmentIDs;
    }

    public Long getFlushTs() {
        return flushTs;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "GetFlushStateParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", segmentIDs=" + segmentIDs +
                ", flushTs=" + flushTs +
                '}';
    }

    /**
     * Builder for {@link GetFlushStateParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;
        private final List<Long> segmentIDs = new ArrayList<>(); // deprecated
        private Long flushTs = 0L;

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
         * Specify segments
         *
         * @param segmentIDs segments id list
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withSegmentIDs(List<Long> segmentIDs) {
            // Replace @NonNull logic with explicit null check
            if (segmentIDs == null) {
                throw new IllegalArgumentException("segmentIDs cannot be null");
            }
            this.segmentIDs.addAll(segmentIDs);
            return this;
        }

        /**
         * Specify a segment
         *
         * @param segmentID segment id
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder addSegmentID(Long segmentID) {
            // Replace @NonNull logic with explicit null check
            if (segmentID == null) {
                throw new IllegalArgumentException("segmentID cannot be null");
            }
            this.segmentIDs.add(segmentID);
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            // Replace @NonNull logic with explicit null check
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Input a time stamp of a flush action, get its flush state
         *
         * @param flushTs a time stamp returned by the flush() response
         * @return <code>Builder</code>
         */
        public Builder withFlushTs(Long flushTs) {
            // Replace @NonNull logic with explicit null check
            if (flushTs == null) {
                throw new IllegalArgumentException("flushTs cannot be null");
            }
            this.flushTs = flushTs;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetFlushStateParam} instance.
         *
         * @return {@link GetFlushStateParam}
         */
        public GetFlushStateParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetFlushStateParam(this);
        }
    }
}
