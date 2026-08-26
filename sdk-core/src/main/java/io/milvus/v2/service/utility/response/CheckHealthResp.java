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

/**
 * Response returned by the {@code checkHealth} API.
 */
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

    /**
     * Returns whether the Milvus instance is healthy.
     *
     * @return {@code true} if the instance is healthy, {@code false} otherwise
     */
    public Boolean getIsHealthy() {
        return isHealthy;
    }

    /**
     * Sets whether the Milvus instance is healthy.
     *
     * @param isHealthy {@code true} if the instance is healthy, {@code false} otherwise
     */
    public void setIsHealthy(Boolean isHealthy) {
        this.isHealthy = isHealthy;
    }

    /**
     * Returns the reasons why the instance is unhealthy.
     *
     * @return the list of health-check failure reasons
     */
    public List<String> getReasons() {
        return reasons;
    }

    /**
     * Sets the reasons why the instance is unhealthy.
     *
     * @param reasons the list of health-check failure reasons
     */
    public void setReasons(List<String> reasons) {
        this.reasons = reasons;
    }

    /**
     * Returns the quota states of the Milvus instance.
     *
     * @return the list of quota states
     */
    public List<String> getQuotaStates() {
        return quotaStates;
    }

    /**
     * Sets the quota states of the Milvus instance.
     *
     * @param quotaStates the list of quota states
     */
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

        /**
         * Sets whether the Milvus instance is healthy.
         *
         * @param isHealthy {@code true} if the instance is healthy, {@code false} otherwise
         * @return this builder
         */
        public CheckHealthRespBuilder isHealthy(Boolean isHealthy) {
            this.isHealthy = isHealthy;
            return this;
        }

        /**
         * Sets the reasons why the instance is unhealthy.
         *
         * @param reasons the list of health-check failure reasons
         * @return this builder
         */
        public CheckHealthRespBuilder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        /**
         * Sets the quota states of the Milvus instance.
         *
         * @param quotaStates the list of quota states
         * @return this builder
         */
        public CheckHealthRespBuilder quotaStates(List<String> quotaStates) {
            this.quotaStates = quotaStates;
            return this;
        }

        /**
         * Builds the {@code CheckHealthResp}.
         *
         * @return the constructed {@code CheckHealthResp}
         */
        public CheckHealthResp build() {
            return new CheckHealthResp(this);
        }
    }
}
