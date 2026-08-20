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

public class GetCompactionPlansResp {
    private Long compactionId;
    private CompactionState state;
    private List<CompactionPlan> plans;

    private GetCompactionPlansResp(GetCompactionPlansRespBuilder builder) {
        this.compactionId = builder.compactionId;
        this.state = builder.state;
        this.plans = builder.plans;
    }

    public static GetCompactionPlansRespBuilder builder() {
        return new GetCompactionPlansRespBuilder();
    }

    public Long getCompactionId() {
        return compactionId;
    }

    public void setCompactionId(Long compactionId) {
        this.compactionId = compactionId;
    }

    public CompactionState getState() {
        return state;
    }

    public List<CompactionPlan> getPlans() {
        return plans;
    }


    @Override
    public String toString() {
        return "GetCompactionPlansResp{" +
                "compactionId=" + compactionId +
                ", state=" + state +
                ", plans=" + plans +
                '}';
    }

    public static class GetCompactionPlansRespBuilder {
        private Long compactionId;
        private CompactionState state = CompactionState.UndefiedState;
        private List<CompactionPlan> plans = new ArrayList<>();

        public GetCompactionPlansRespBuilder compactionId(Long compactionId) {
            this.compactionId = compactionId;
            return this;
        }

        public GetCompactionPlansRespBuilder state(CompactionState state) {
            this.state = state;
            return this;
        }

        public GetCompactionPlansRespBuilder plans(List<CompactionPlan> plans) {
            this.plans = plans;
            return this;
        }

        public GetCompactionPlansResp build() {
            return new GetCompactionPlansResp(this);
        }
    }
}
