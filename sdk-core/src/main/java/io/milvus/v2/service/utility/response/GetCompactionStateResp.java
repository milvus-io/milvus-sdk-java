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

    public CompactionState getState() {
        return state;
    }

    public void setState(CompactionState state) {
        this.state = state;
    }

    public Long getExecutingPlanNo() {
        return executingPlanNo;
    }

    public void setExecutingPlanNo(Long executingPlanNo) {
        this.executingPlanNo = executingPlanNo;
    }

    public Long getTimeoutPlanNo() {
        return timeoutPlanNo;
    }

    public void setTimeoutPlanNo(Long timeoutPlanNo) {
        this.timeoutPlanNo = timeoutPlanNo;
    }

    public Long getCompletedPlanNo() {
        return completedPlanNo;
    }

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

        public GetCompactionStateRespBuilder state(CompactionState state) {
            this.state = state;
            return this;
        }

        public GetCompactionStateRespBuilder executingPlanNo(Long executingPlanNo) {
            this.executingPlanNo = executingPlanNo;
            return this;
        }

        public GetCompactionStateRespBuilder timeoutPlanNo(Long timeoutPlanNo) {
            this.timeoutPlanNo = timeoutPlanNo;
            return this;
        }

        public GetCompactionStateRespBuilder completedPlanNo(Long completedPlanNo) {
            this.completedPlanNo = completedPlanNo;
            return this;
        }

        public GetCompactionStateResp build() {
            return new GetCompactionStateResp(this);
        }
    }
}
