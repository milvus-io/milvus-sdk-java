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
 * Response of the {@code cancelRequests} API.
 *
 * <p>Every requested id is in exactly one of {@link #getCanceled()}, {@link #getNotFound()} and
 * {@link #getUndetermined()}. Cancellation is asynchronous: a canceled request stops at its next
 * cancellation check, usually within milliseconds, and until then {@code listRunningRequests} shows it
 * in state {@code Canceling}.</p>
 */
public class CancelRequestsResp {
    private List<RunningRequestInfo> canceled;
    private List<String> notFound;
    private List<String> undetermined;
    private List<RunningRequestNodeResult> nodeResults;

    private CancelRequestsResp(CancelRequestsRespBuilder builder) {
        this.canceled = builder.canceled;
        this.notFound = builder.notFound;
        this.undetermined = builder.undetermined;
        this.nodeResults = builder.nodeResults;
    }

    /**
     * Creates a new builder for {@code CancelRequestsResp}.
     *
     * @return the builder
     */
    public static CancelRequestsRespBuilder builder() {
        return new CancelRequestsRespBuilder();
    }

    /**
     * Returns the canceled requests, each as it was at the moment it was canceled.
     *
     * @return the canceled
     */
    public List<RunningRequestInfo> getCanceled() {
        return canceled;
    }

    /**
     * Sets the canceled requests, each as it was at the moment it was canceled.
     *
     * @param canceled the canceled
     */
    public void setCanceled(List<RunningRequestInfo> canceled) {
        this.canceled = canceled;
    }

    /**
     * Returns the ids no proxy holds, known because every proxy answered: the request already finished,
     * never existed, or was issued by another user.
     *
     * @return the notFound
     */
    public List<String> getNotFound() {
        return notFound;
    }

    /**
     * Sets the ids no proxy holds, known because every proxy answered: the request already finished,
     * never existed, or was issued by another user.
     *
     * @param notFound the notFound
     */
    public void setNotFound(List<String> notFound) {
        this.notFound = notFound;
    }

    /**
     * Returns the ids not canceled while some proxy did not answer: each may be running on that proxy or may
     * have finished. Retry the cancel, or list again, once the proxy is back.
     *
     * @return the undetermined
     */
    public List<String> getUndetermined() {
        return undetermined;
    }

    /**
     * Sets the ids not canceled while some proxy did not answer: each may be running on that proxy or may
     * have finished. Retry the cancel, or list again, once the proxy is back.
     *
     * @param undetermined the undetermined
     */
    public void setUndetermined(List<String> undetermined) {
        this.undetermined = undetermined;
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
        return "CancelRequestsResp{" +
                "canceled=" + canceled +
                ", notFound=" + notFound +
                ", undetermined=" + undetermined +
                ", nodeResults=" + nodeResults +
                '}';
    }

    /**
     * Builder for {@link CancelRequestsResp}.
     */
    public static class CancelRequestsRespBuilder {
        private List<RunningRequestInfo> canceled = new ArrayList<>();
        private List<String> notFound = new ArrayList<>();
        private List<String> undetermined = new ArrayList<>();
        private List<RunningRequestNodeResult> nodeResults = new ArrayList<>();

        /**
         * Sets the canceled requests, each as it was at the moment it was canceled.
         *
         * @param canceled the canceled
         * @return this builder
         */
        public CancelRequestsRespBuilder canceled(List<RunningRequestInfo> canceled) {
            this.canceled = canceled;
            return this;
        }

        /**
         * Sets the ids no proxy holds, known because every proxy answered: the request already finished,
         * never existed, or was issued by another user.
         *
         * @param notFound the notFound
         * @return this builder
         */
        public CancelRequestsRespBuilder notFound(List<String> notFound) {
            this.notFound = notFound;
            return this;
        }

        /**
         * Sets the ids not canceled while some proxy did not answer: each may be running on that proxy or may
         * have finished. Retry the cancel, or list again, once the proxy is back.
         *
         * @param undetermined the undetermined
         * @return this builder
         */
        public CancelRequestsRespBuilder undetermined(List<String> undetermined) {
            this.undetermined = undetermined;
            return this;
        }

        /**
         * Sets the outcome on each proxy.
         *
         * @param nodeResults the nodeResults
         * @return this builder
         */
        public CancelRequestsRespBuilder nodeResults(List<RunningRequestNodeResult> nodeResults) {
            this.nodeResults = nodeResults;
            return this;
        }

        /**
         * Builds the {@code CancelRequestsResp}.
         *
         * @return the constructed {@code CancelRequestsResp}
         */
        public CancelRequestsResp build() {
            return new CancelRequestsResp(this);
        }
    }
}
