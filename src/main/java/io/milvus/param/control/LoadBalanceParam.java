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
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>loadBalance</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+17+--+Support+handoff+and+load+balance+segment+on+query+nodes">Handoff and load balance</a>
 */
@Getter
public class LoadBalanceParam {
    private final Long srcNodeID;
    private final List<Long> destNodeIDs;
    private final List<Long> segmentIDs;

    private LoadBalanceParam(@NonNull Builder builder) {
        this.srcNodeID = builder.srcNodeID;
        this.destNodeIDs = builder.destNodeIDs;
        this.segmentIDs = builder.segmentIDs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Constructs a <code>String</code> by {@link LoadBalanceParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "LoadBalanceParam{" +
                "srcNodeID='" + srcNodeID + '\'' +
                "destNodeIDs='" + destNodeIDs.toString() + '\'' +
                "segmentIDs='" + segmentIDs.toString() + '\'' +
                '}';
    }

    /**
     * Builder for {@link LoadBalanceParam} class.
     */
    public static final class Builder {
        private final List<Long> destNodeIDs = new ArrayList<>();
        private final List<Long> segmentIDs = new ArrayList<>();
        private Long srcNodeID;

        private Builder() {
        }

        /**
         * Sets the source query node by ID in which the sealed segments were loaded.
         *
         * @param srcNodeID source query node id
         * @return <code>Builder</code>
         */
        public Builder withSourceNodeID(@NonNull Long srcNodeID) {
            this.srcNodeID = srcNodeID;
            return this;
        }

        /**
         * Adds the destination query node by ID to which the sealed segments will be balanced.
         *
         * @param destNodeID destination query node id
         * @return <code>Builder</code>
         */
        public Builder addDestinationNodeID(@NonNull Long destNodeID) {
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
        public Builder withDestinationNodeID(@NonNull List<Long> destNodeIDs) {
            destNodeIDs.forEach(this::addDestinationNodeID);
            return this;
        }

        /**
         * Adds a sealed segments by ID to be balanced.
         *
         * @param segmentID sealed segment id
         * @return <code>Builder</code>
         */
        public Builder addSegmentID(@NonNull Long segmentID) {
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
        public Builder withSegmentIDs(@NonNull List<Long> segmentIDs) {
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
