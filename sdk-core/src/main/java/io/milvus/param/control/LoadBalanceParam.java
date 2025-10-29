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

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>loadBalance</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+17+--+Support+handoff+and+load+balance+segment+on+query+nodes">Handoff and load balance</a>
 */
public class LoadBalanceParam {
    private final String databaseName;
    private final String collectionName;
    private final Long srcNodeID;
    private final List<Long> destNodeIDs;
    private final List<Long> segmentIDs;

    private LoadBalanceParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.srcNodeID = builder.srcNodeID;
        this.destNodeIDs = builder.destNodeIDs;
        this.segmentIDs = builder.segmentIDs;
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

    public Long getSrcNodeID() {
        return srcNodeID;
    }

    public List<Long> getDestNodeIDs() {
        return destNodeIDs;
    }

    public List<Long> getSegmentIDs() {
        return segmentIDs;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "LoadBalanceParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", srcNodeID=" + srcNodeID +
                ", destNodeIDs=" + destNodeIDs +
                ", segmentIDs=" + segmentIDs +
                '}';
    }

    /**
     * Builder for {@link LoadBalanceParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;
        private final List<Long> destNodeIDs = new ArrayList<>();
        private final List<Long> segmentIDs = new ArrayList<>();
        private Long srcNodeID;

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
         * Sets the source query node by ID in which the sealed segments were loaded.
         *
         * @param srcNodeID source query node id
         * @return <code>Builder</code>
         */
        public Builder withSourceNodeID(Long srcNodeID) {
            // Replace @NonNull logic with explicit null check
            if (srcNodeID == null) {
                throw new IllegalArgumentException("srcNodeID cannot be null");
            }
            this.srcNodeID = srcNodeID;
            return this;
        }

        /**
         * Adds the destination query node by ID to which the sealed segments will be balanced.
         *
         * @param destNodeID destination query node id
         * @return <code>Builder</code>
         */
        public Builder addDestinationNodeID(Long destNodeID) {
            // Replace @NonNull logic with explicit null check
            if (destNodeID == null) {
                throw new IllegalArgumentException("destNodeID cannot be null");
            }
            if (!destNodeIDs.contains(destNodeID)) {
                destNodeIDs.add(destNodeID);
            }

            return this;
        }

        /**
         * Sets the destination query node by ID array to which the sealed segments will be balance.
         *
         * @param destNodeIDs destination query node id array
         * @return <code>Builder</code>
         */
        public Builder withDestinationNodeID(List<Long> destNodeIDs) {
            // Replace @NonNull logic with explicit null check
            if (destNodeIDs == null) {
                throw new IllegalArgumentException("destNodeIDs cannot be null");
            }
            destNodeIDs.forEach(this::addDestinationNodeID);
            return this;
        }

        /**
         * Adds a sealed segments by ID to be balanced.
         *
         * @param segmentID sealed segment id
         * @return <code>Builder</code>
         */
        public Builder addSegmentID(Long segmentID) {
            // Replace @NonNull logic with explicit null check
            if (segmentID == null) {
                throw new IllegalArgumentException("segmentID cannot be null");
            }
            if (!segmentIDs.contains(segmentID)) {
                segmentIDs.add(segmentID);
            }

            return this;
        }

        /**
         * Sets a sealed segments by ID array to be balanced.
         *
         * @param segmentIDs sealed segments id array
         * @return <code>Builder</code>
         */
        public Builder withSegmentIDs(List<Long> segmentIDs) {
            // Replace @NonNull logic with explicit null check
            if (segmentIDs == null) {
                throw new IllegalArgumentException("segmentIDs cannot be null");
            }
            segmentIDs.forEach(this::addSegmentID);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link LoadBalanceParam} instance.
         *
         * @return {@link LoadBalanceParam}
         */
        public LoadBalanceParam build() throws ParamException {
            if (segmentIDs.isEmpty()) {
                throw new ParamException("Sealed segment id array cannot be empty");
            }

            if (destNodeIDs.isEmpty()) {
                throw new ParamException("Destination query node id array cannot be empty");
            }

            return new LoadBalanceParam(this);
        }
    }
}
