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

import java.util.ArrayList;
import java.util.List;

public class CheckHealthResp {
    private Boolean isHealthy;
    private List<String> reasons;
    private List<String> quotaStates;

    private CheckHealthResp(CheckHealthRespBuilder builder) {
        this.isHealthy = builder.isHealthy;
        this.reasons = builder.reasons;
        this.quotaStates = builder.quotaStates;
    }

    public static CheckHealthRespBuilder builder() {
        return new CheckHealthRespBuilder();
    }

    public Boolean getIsHealthy() {
        return isHealthy;
    }

    public void setIsHealthy(Boolean isHealthy) {
        this.isHealthy = isHealthy;
    }

    public List<String> getReasons() {
        return reasons;
    }

    public void setReasons(List<String> reasons) {
        this.reasons = reasons;
    }

    public List<String> getQuotaStates() {
        return quotaStates;
    }

    public void setQuotaStates(List<String> quotaStates) {
        this.quotaStates = quotaStates;
    }

    @Override
    public String toString() {
        return "CheckHealthResp{" +
                "isHealthy=" + isHealthy +
                ", reasons=" + reasons +
                ", quotaStates=" + quotaStates +
                '}';
    }

    public static class CheckHealthRespBuilder {
        private Boolean isHealthy = false;
        private List<String> reasons = new ArrayList<>();
        private List<String> quotaStates = new ArrayList<>();

        public CheckHealthRespBuilder isHealthy(Boolean isHealthy) {
            this.isHealthy = isHealthy;
            return this;
        }

        public CheckHealthRespBuilder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public CheckHealthRespBuilder quotaStates(List<String> quotaStates) {
            this.quotaStates = quotaStates;
            return this;
        }

        public CheckHealthResp build() {
            return new CheckHealthResp(this);
        }
    }
}
