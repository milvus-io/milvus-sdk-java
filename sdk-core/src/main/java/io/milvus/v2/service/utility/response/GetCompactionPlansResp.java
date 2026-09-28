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

package io.milvus.v2.service.utility.response;

import io.milvus.v2.common.CompactionPlan;
import io.milvus.v2.common.CompactionState;

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code getCompactionPlans} API.
 */


public class GetCompactionPlansResp {
    private Long compactionId;
    private String collectionName;
    private CompactionState state;
    private List<CompactionPlan> plans;

    private GetCompactionPlansResp(GetCompactionPlansRespBuilder builder) {
        this.compactionId = builder.compactionId;
        this.collectionName = builder.collectionName;
        this.state = builder.state;
        this.plans = builder.plans;
    }

    /**
     * Creates a new builder for {@code GetCompactionPlansResp}.
     *
     * @return the builder
     */


    public static GetCompactionPlansRespBuilder builder() {
        return new GetCompactionPlansRespBuilder();
    }

    /**
     * Returns the compaction ID.
     *
     * @return the compaction ID
     */


    public Long getCompactionId() {
        return compactionId;
    }

    /**
     * Sets the compaction ID.
     *
     * @param compactionId the compaction ID
     */


    public void setCompactionId(Long compactionId) {
        this.compactionId = compactionId;
    }

    /**
     * Returns the name of the collection the compaction plans belong to.
     *
     * @return the collection name
     */


    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection the compaction plans belong to.
     *
     * @param collectionName the collection name
     */


    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the compaction state.
     *
     * @return the compaction state
     */


    public CompactionState getState() {
        return state;
    }

    /**
     * Returns the compaction plans of the compaction.
     *
     * @return the list of compaction plans
     */


    public List<CompactionPlan> getPlans() {
        return plans;
    }


    @Override
    public String toString() {
        return "GetCompactionPlansResp{" +
                "compactionId=" + compactionId +
                ", collectionName='" + collectionName + '\'' +
                ", state=" + state +
                ", plans=" + plans +
                '}';
    }

    /**
     * Builder for {@link GetCompactionPlansResp} class.
     */


    public static class GetCompactionPlansRespBuilder {
        private Long compactionId;
        private String collectionName;
        private CompactionState state = CompactionState.UndefiedState;
        private List<CompactionPlan> plans = new ArrayList<>();

        /**
         * Sets the compaction ID.
         *
         * @param compactionId the compaction ID
         * @return this builder
         */


        public GetCompactionPlansRespBuilder compactionId(Long compactionId) {
            this.compactionId = compactionId;
            return this;
        }

        /**
         * Sets the name of the collection the compaction plans belong to.
         *
         * @param collectionName the collection name
         * @return this builder
         */


        public GetCompactionPlansRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the compaction state.
         *
         * @param state the compaction state
         * @return this builder
         */


        public GetCompactionPlansRespBuilder state(CompactionState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the compaction plans of the compaction.
         *
         * @param plans the list of compaction plans
         * @return this builder
         */


        public GetCompactionPlansRespBuilder plans(List<CompactionPlan> plans) {
            this.plans = plans;
            return this;
        }

        /**
         * Builds the {@code GetCompactionPlansResp}.
         *
         * @return the constructed {@code GetCompactionPlansResp}
         */


        public GetCompactionPlansResp build() {
            return new GetCompactionPlansResp(this);
        }
    }
}
