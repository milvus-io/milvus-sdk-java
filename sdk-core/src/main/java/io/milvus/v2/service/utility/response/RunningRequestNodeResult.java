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

/**
 * The outcome of a {@code listRunningRequests} or {@code cancelRequests} call on one proxy.
 *
 * <p>Both calls are sent to every proxy. A proxy that did not answer contributed nothing to the
 * result; its requests are missing from it.</p>
 */
public class RunningRequestNodeResult {
    private Long nodeId;
    private Integer errorCode;
    private String reason;
    private Boolean unimplemented;

    private RunningRequestNodeResult(RunningRequestNodeResultBuilder builder) {
        this.nodeId = builder.nodeId;
        this.errorCode = builder.errorCode;
        this.reason = builder.reason;
        this.unimplemented = builder.unimplemented;
    }

    /**
     * Creates a new builder for {@code RunningRequestNodeResult}.
     *
     * @return the builder
     */
    public static RunningRequestNodeResultBuilder builder() {
        return new RunningRequestNodeResultBuilder();
    }

    /**
     * Returns the node id of the proxy.
     *
     * @return the nodeId
     */
    public Long getNodeId() {
        return nodeId;
    }

    /**
     * Sets the node id of the proxy.
     *
     * @param nodeId the nodeId
     */
    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Returns the error code of the proxy's answer; 0 means the proxy answered.
     *
     * @return the errorCode
     */
    public Integer getErrorCode() {
        return errorCode;
    }

    /**
     * Sets the error code of the proxy's answer; 0 means the proxy answered.
     *
     * @param errorCode the errorCode
     */
    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Returns the error message when the proxy did not answer.
     *
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Sets the error message when the proxy did not answer.
     *
     * @param reason the reason
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * Returns whether the proxy is too old to serve the call, as during a rolling upgrade. Such a proxy holds
     * no listable requests.
     *
     * @return the unimplemented
     */
    public Boolean getUnimplemented() {
        return unimplemented;
    }

    /**
     * Sets whether the proxy is too old to serve the call, as during a rolling upgrade. Such a proxy holds
     * no listable requests.
     *
     * @param unimplemented the unimplemented
     */
    public void setUnimplemented(Boolean unimplemented) {
        this.unimplemented = unimplemented;
    }

    /**
     * Returns whether the proxy answered the call.
     *
     * @return {@code true} when {@link #getErrorCode()} is 0
     */
    public boolean isSuccess() {
        return errorCode == null || errorCode == 0;
    }

    @Override
    public String toString() {
        return "RunningRequestNodeResult{" +
                "nodeId=" + nodeId +
                ", errorCode=" + errorCode +
                ", reason='" + reason + '\'' +
                ", unimplemented=" + unimplemented +
                '}';
    }

    /**
     * Builder for {@link RunningRequestNodeResult}.
     */
    public static class RunningRequestNodeResultBuilder {
        private Long nodeId = 0L;
        private Integer errorCode = 0;
        private String reason = "";
        private Boolean unimplemented = Boolean.FALSE;

        /**
         * Sets the node id of the proxy.
         *
         * @param nodeId the nodeId
         * @return this builder
         */
        public RunningRequestNodeResultBuilder nodeId(Long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         * Sets the error code of the proxy's answer; 0 means the proxy answered.
         *
         * @param errorCode the errorCode
         * @return this builder
         */
        public RunningRequestNodeResultBuilder errorCode(Integer errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        /**
         * Sets the error message when the proxy did not answer.
         *
         * @param reason the reason
         * @return this builder
         */
        public RunningRequestNodeResultBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Sets whether the proxy is too old to serve the call, as during a rolling upgrade. Such a proxy holds
         * no listable requests.
         *
         * @param unimplemented the unimplemented
         * @return this builder
         */
        public RunningRequestNodeResultBuilder unimplemented(Boolean unimplemented) {
            this.unimplemented = unimplemented;
            return this;
        }

        /**
         * Builds the {@code RunningRequestNodeResult}.
         *
         * @return the constructed {@code RunningRequestNodeResult}
         */
        public RunningRequestNodeResult build() {
            return new RunningRequestNodeResult(this);
        }
    }
}
