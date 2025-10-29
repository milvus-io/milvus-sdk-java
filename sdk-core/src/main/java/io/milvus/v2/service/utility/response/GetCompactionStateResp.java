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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GetCompactionStateResp {
    private CompactionState state;
    private Long executingPlanNo;
    private Long timeoutPlanNo;
    private Long completedPlanNo;

    private GetCompactionStateResp(Builder builder) {
        this.state = builder.state;
        this.executingPlanNo = builder.executingPlanNo;
        this.timeoutPlanNo = builder.timeoutPlanNo;
        this.completedPlanNo = builder.completedPlanNo;
    }

    public static Builder builder() {
        return new Builder();
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetCompactionStateResp that = (GetCompactionStateResp) obj;
        return new EqualsBuilder()
                .append(state, that.state)
                .append(executingPlanNo, that.executingPlanNo)
                .append(timeoutPlanNo, that.timeoutPlanNo)
                .append(completedPlanNo, that.completedPlanNo)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(state)
                .append(executingPlanNo)
                .append(timeoutPlanNo)
                .append(completedPlanNo)
                .toHashCode();
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

    public static class Builder {
        private CompactionState state = CompactionState.UndefiedState;
        private Long executingPlanNo = 0L;
        private Long timeoutPlanNo = 0L;
        private Long completedPlanNo = 0L;

        public Builder state(CompactionState state) {
            this.state = state;
            return this;
        }

        public Builder executingPlanNo(Long executingPlanNo) {
            this.executingPlanNo = executingPlanNo;
            return this;
        }

        public Builder timeoutPlanNo(Long timeoutPlanNo) {
            this.timeoutPlanNo = timeoutPlanNo;
            return this;
        }

        public Builder completedPlanNo(Long completedPlanNo) {
            this.completedPlanNo = completedPlanNo;
            return this;
        }

        public GetCompactionStateResp build() {
            return new GetCompactionStateResp(this);
        }
    }
}
