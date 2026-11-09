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
 * Response of the {@code listRunningRequests} API.
 *
 * <p>The call succeeds as long as at least one proxy answered. Check {@link #getNodeResults()} to tell
 * a complete answer from a partial one.</p>
 */
public class ListRunningRequestsResp {
    private List<RunningRequestInfo> requests;
    private List<RunningRequestNodeResult> nodeResults;

    private ListRunningRequestsResp(ListRunningRequestsRespBuilder builder) {
        this.requests = builder.requests;
        this.nodeResults = builder.nodeResults;
    }

    /**
     * Creates a new builder for {@code ListRunningRequestsResp}.
     *
     * @return the builder
     */
    public static ListRunningRequestsRespBuilder builder() {
        return new ListRunningRequestsRespBuilder();
    }

    /**
     * Returns the requests running on the proxies that answered.
     *
     * @return the requests
     */
    public List<RunningRequestInfo> getRequests() {
        return requests;
    }

    /**
     * Sets the requests running on the proxies that answered.
     *
     * @param requests the requests
     */
    public void setRequests(List<RunningRequestInfo> requests) {
        this.requests = requests;
    }

    /**
     * Returns the outcome on each proxy.
     *
     * @return the nodeResults
     */
    public List<RunningRequestNodeResult> getNodeResults() {
        return nodeResults;
    }

    /**
     * Sets the outcome on each proxy.
     *
     * @param nodeResults the nodeResults
     */
    public void setNodeResults(List<RunningRequestNodeResult> nodeResults) {
        this.nodeResults = nodeResults;
    }

    @Override
    public String toString() {
        return "ListRunningRequestsResp{" +
                "requests=" + requests +
                ", nodeResults=" + nodeResults +
                '}';
    }

    /**
     * Builder for {@link ListRunningRequestsResp}.
     */
    public static class ListRunningRequestsRespBuilder {
        private List<RunningRequestInfo> requests = new ArrayList<>();
        private List<RunningRequestNodeResult> nodeResults = new ArrayList<>();

        /**
         * Sets the requests running on the proxies that answered.
         *
         * @param requests the requests
         * @return this builder
         */
        public ListRunningRequestsRespBuilder requests(List<RunningRequestInfo> requests) {
            this.requests = requests;
            return this;
        }

        /**
         * Sets the outcome on each proxy.
         *
         * @param nodeResults the nodeResults
         * @return this builder
         */
        public ListRunningRequestsRespBuilder nodeResults(List<RunningRequestNodeResult> nodeResults) {
            this.nodeResults = nodeResults;
            return this;
        }

        /**
         * Builds the {@code ListRunningRequestsResp}.
         *
         * @return the constructed {@code ListRunningRequestsResp}
         */
        public ListRunningRequestsResp build() {
            return new ListRunningRequestsResp(this);
        }
    }
}
