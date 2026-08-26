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

import io.milvus.v2.common.CompactionState;

/**
 * Response returned by the {@code getCompactionState} API.
 */
public class GetCompactionStateResp {
    private CompactionState state;
    private Long executingPlanNo;
    private Long timeoutPlanNo;
    private Long completedPlanNo;

    private GetCompactionStateResp(GetCompactionStateRespBuilder builder) {
        this.state = builder.state;
        this.executingPlanNo = builder.executingPlanNo;
        this.timeoutPlanNo = builder.timeoutPlanNo;
        this.completedPlanNo = builder.completedPlanNo;
    }

    public static GetCompactionStateRespBuilder builder() {
        return new GetCompactionStateRespBuilder();
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
     * Sets the compaction state.
     *
     * @param state the compaction state
     */
    public void setState(CompactionState state) {
        this.state = state;
    }

    /**
     * Returns the number of compaction plans currently executing.
     *
     * @return the number of executing compaction plans
     */
    public Long getExecutingPlanNo() {
        return executingPlanNo;
    }

    /**
     * Sets the number of compaction plans currently executing.
     *
     * @param executingPlanNo the number of executing compaction plans
     */
    public void setExecutingPlanNo(Long executingPlanNo) {
        this.executingPlanNo = executingPlanNo;
    }

    /**
     * Returns the number of compaction plans that timed out.
     *
     * @return the number of timed-out compaction plans
     */
    public Long getTimeoutPlanNo() {
        return timeoutPlanNo;
    }

    /**
     * Sets the number of compaction plans that timed out.
     *
     * @param timeoutPlanNo the number of timed-out compaction plans
     */
    public void setTimeoutPlanNo(Long timeoutPlanNo) {
        this.timeoutPlanNo = timeoutPlanNo;
    }

    /**
     * Returns the number of completed compaction plans.
     *
     * @return the number of completed compaction plans
     */
    public Long getCompletedPlanNo() {
        return completedPlanNo;
    }

    /**
     * Sets the number of completed compaction plans.
     *
     * @param completedPlanNo the number of completed compaction plans
     */
    public void setCompletedPlanNo(Long completedPlanNo) {
        this.completedPlanNo = completedPlanNo;
    }

    @Override
    public String toString() {
        return "GetCompactionStateResp{" +
                "state=" + state +
                ", executingPlanNo=" + executingPlanNo +
                ", timeoutPlanNo=" + timeoutPlanNo +
                ", completedPlanNo=" + completedPlanNo +
                '}';
    }

    public static class GetCompactionStateRespBuilder {
        private CompactionState state = CompactionState.UndefiedState;
        private Long executingPlanNo = 0L;
        private Long timeoutPlanNo = 0L;
        private Long completedPlanNo = 0L;

        /**
         * Sets the compaction state.
         *
         * @param state the compaction state
         * @return this builder
         */
        public GetCompactionStateRespBuilder state(CompactionState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the number of compaction plans currently executing.
         *
         * @param executingPlanNo the number of executing compaction plans
         * @return this builder
         */
        public GetCompactionStateRespBuilder executingPlanNo(Long executingPlanNo) {
            this.executingPlanNo = executingPlanNo;
            return this;
        }

        /**
         * Sets the number of compaction plans that timed out.
         *
         * @param timeoutPlanNo the number of timed-out compaction plans
         * @return this builder
         */
        public GetCompactionStateRespBuilder timeoutPlanNo(Long timeoutPlanNo) {
            this.timeoutPlanNo = timeoutPlanNo;
            return this;
        }

        /**
         * Sets the number of completed compaction plans.
         *
         * @param completedPlanNo the number of completed compaction plans
         * @return this builder
         */
        public GetCompactionStateRespBuilder completedPlanNo(Long completedPlanNo) {
            this.completedPlanNo = completedPlanNo;
            return this;
        }

        /**
         * Builds the {@code GetCompactionStateResp}.
         *
         * @return the constructed {@code GetCompactionStateResp}
         */
        public GetCompactionStateResp build() {
            return new GetCompactionStateResp(this);
        }
    }
}
