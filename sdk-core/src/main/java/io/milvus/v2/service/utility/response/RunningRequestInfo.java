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
 * One Search, HybridSearch or Query request that a proxy is executing.
 */
public class RunningRequestInfo {
    private String requestId;
    private Long proxyId;
    private String type;
    private String databaseName;
    private String collectionName;
    private String user;
    private String clientAddr;
    private Long nq;
    private Long topK;
    private String expr;
    private Long startTimeMs;
    private Long queuedMs;
    private Long elapsedMs;
    private String state;
    private List<Long> taskIds;
    private String traceId;
    private Boolean cancellable;

    private RunningRequestInfo(RunningRequestInfoBuilder builder) {
        this.requestId = builder.requestId;
        this.proxyId = builder.proxyId;
        this.type = builder.type;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.user = builder.user;
        this.clientAddr = builder.clientAddr;
        this.nq = builder.nq;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.startTimeMs = builder.startTimeMs;
        this.queuedMs = builder.queuedMs;
        this.elapsedMs = builder.elapsedMs;
        this.state = builder.state;
        this.taskIds = builder.taskIds;
        this.traceId = builder.traceId;
        this.cancellable = builder.cancellable;
    }

    /**
     * Creates a new builder for {@code RunningRequestInfo}.
     *
     * @return the builder
     */
    public static RunningRequestInfoBuilder builder() {
        return new RunningRequestInfoBuilder();
    }

    /**
     * Returns the request id, an opaque string such as {@code "25-to03zu-rs"}; pass it to
     * {@code cancelRequests} unchanged.
     *
     * @return the requestId
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Sets the request id, an opaque string such as {@code "25-to03zu-rs"}; pass it to
     * {@code cancelRequests} unchanged.
     *
     * @param requestId the requestId
     */
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * Returns the node id of the proxy that holds the request.
     *
     * @return the proxyId
     */
    public Long getProxyId() {
        return proxyId;
    }

    /**
     * Sets the node id of the proxy that holds the request.
     *
     * @param proxyId the proxyId
     */
    public void setProxyId(Long proxyId) {
        this.proxyId = proxyId;
    }

    /**
     * Returns the request type: {@code Search}, {@code HybridSearch} or {@code Query}.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the request type: {@code Search}, {@code HybridSearch} or {@code Query}.
     *
     * @param type the type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the database name.
     *
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the databaseName
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collectionName
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collectionName
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the user who issued the request.
     *
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user who issued the request.
     *
     * @param user the user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Returns the client address seen by the proxy.
     *
     * @return the clientAddr
     */
    public String getClientAddr() {
        return clientAddr;
    }

    /**
     * Sets the client address seen by the proxy.
     *
     * @param clientAddr the clientAddr
     */
    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    /**
     * Returns the number of query vectors.
     *
     * @return the nq
     */
    public Long getNq() {
        return nq;
    }

    /**
     * Sets the number of query vectors.
     *
     * @param nq the nq
     */
    public void setNq(Long nq) {
        this.nq = nq;
    }

    /**
     * Returns the top-k of the request.
     *
     * @return the topK
     */
    public Long getTopK() {
        return topK;
    }

    /**
     * Sets the top-k of the request.
     *
     * @param topK the topK
     */
    public void setTopK(Long topK) {
        this.topK = topK;
    }

    /**
     * Returns the leading part (at most 256 bytes) of the filter expression.
     *
     * @return the expr
     */
    public String getExpr() {
        return expr;
    }

    /**
     * Sets the leading part (at most 256 bytes) of the filter expression.
     *
     * @param expr the expr
     */
    public void setExpr(String expr) {
        this.expr = expr;
    }

    /**
     * Returns the time the request started, in milliseconds since the Unix epoch.
     *
     * @return the startTimeMs
     */
    public Long getStartTimeMs() {
        return startTimeMs;
    }

    /**
     * Sets the time the request started, in milliseconds since the Unix epoch.
     *
     * @param startTimeMs the startTimeMs
     */
    public void setStartTimeMs(Long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    /**
     * Returns how long the request's first task waited in the proxy queue, in milliseconds; 0 until that
     * task starts.
     *
     * @return the queuedMs
     */
    public Long getQueuedMs() {
        return queuedMs;
    }

    /**
     * Sets how long the request's first task waited in the proxy queue, in milliseconds; 0 until that
     * task starts.
     *
     * @param queuedMs the queuedMs
     */
    public void setQueuedMs(Long queuedMs) {
        this.queuedMs = queuedMs;
    }

    /**
     * Returns how long the request has been running, in milliseconds. In a {@code cancelRequests} answer it
     * is how long the request had been running when it was canceled.
     *
     * @return the elapsedMs
     */
    public Long getElapsedMs() {
        return elapsedMs;
    }

    /**
     * Sets how long the request has been running, in milliseconds. In a {@code cancelRequests} answer it
     * is how long the request had been running when it was canceled.
     *
     * @param elapsedMs the elapsedMs
     */
    public void setElapsedMs(Long elapsedMs) {
        this.elapsedMs = elapsedMs;
    }

    /**
     * Returns the request state: {@code Queued}, {@code Running} or {@code Canceling}. In a
     * {@code cancelRequests} answer it is the state before the cancel, or {@code Canceling} when the
     * call repeated an earlier cancel.
     *
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the request state: {@code Queued}, {@code Running} or {@code Canceling}. In a
     * {@code cancelRequests} answer it is the state before the cancel, or {@code Canceling} when the
     * call repeated an earlier cancel.
     *
     * @param state the state
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Returns the ids of the proxy tasks the request has created.
     *
     * @return the taskIds
     */
    public List<Long> getTaskIds() {
        return taskIds;
    }

    /**
     * Sets the ids of the proxy tasks the request has created.
     *
     * @param taskIds the taskIds
     */
    public void setTaskIds(List<Long> taskIds) {
        this.taskIds = taskIds;
    }

    /**
     * Returns the trace id of the request.
     *
     * @return the traceId
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the trace id of the request.
     *
     * @param traceId the traceId
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Returns whether the request can be canceled.
     *
     * @return the cancellable
     */
    public Boolean getCancellable() {
        return cancellable;
    }

    /**
     * Sets whether the request can be canceled.
     *
     * @param cancellable the cancellable
     */
    public void setCancellable(Boolean cancellable) {
        this.cancellable = cancellable;
    }

    @Override
    public String toString() {
        return "RunningRequestInfo{" +
                "requestId='" + requestId + '\'' +
                ", proxyId=" + proxyId +
                ", type='" + type + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", user='" + user + '\'' +
                ", clientAddr='" + clientAddr + '\'' +
                ", nq=" + nq +
                ", topK=" + topK +
                ", expr='" + expr + '\'' +
                ", startTimeMs=" + startTimeMs +
                ", queuedMs=" + queuedMs +
                ", elapsedMs=" + elapsedMs +
                ", state='" + state + '\'' +
                ", taskIds=" + taskIds +
                ", traceId='" + traceId + '\'' +
                ", cancellable=" + cancellable +
                '}';
    }

    /**
     * Builder for {@link RunningRequestInfo}.
     */
    public static class RunningRequestInfoBuilder {
        private String requestId = "";
        private Long proxyId = 0L;
        private String type = "";
        private String databaseName = "";
        private String collectionName = "";
        private String user = "";
        private String clientAddr = "";
        private Long nq = 0L;
        private Long topK = 0L;
        private String expr = "";
        private Long startTimeMs = 0L;
        private Long queuedMs = 0L;
        private Long elapsedMs = 0L;
        private String state = "";
        private List<Long> taskIds = new ArrayList<>();
        private String traceId = "";
        private Boolean cancellable = Boolean.FALSE;

        /**
         * Sets the request id, an opaque string such as {@code "25-to03zu-rs"}; pass it to
         * {@code cancelRequests} unchanged.
         *
         * @param requestId the requestId
         * @return this builder
         */
        public RunningRequestInfoBuilder requestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        /**
         * Sets the node id of the proxy that holds the request.
         *
         * @param proxyId the proxyId
         * @return this builder
         */
        public RunningRequestInfoBuilder proxyId(Long proxyId) {
            this.proxyId = proxyId;
            return this;
        }

        /**
         * Sets the request type: {@code Search}, {@code HybridSearch} or {@code Query}.
         *
         * @param type the type
         * @return this builder
         */
        public RunningRequestInfoBuilder type(String type) {
            this.type = type;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the databaseName
         * @return this builder
         */
        public RunningRequestInfoBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collectionName
         * @return this builder
         */
        public RunningRequestInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the user who issued the request.
         *
         * @param user the user
         * @return this builder
         */
        public RunningRequestInfoBuilder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * Sets the client address seen by the proxy.
         *
         * @param clientAddr the clientAddr
         * @return this builder
         */
        public RunningRequestInfoBuilder clientAddr(String clientAddr) {
            this.clientAddr = clientAddr;
            return this;
        }

        /**
         * Sets the number of query vectors.
         *
         * @param nq the nq
         * @return this builder
         */
        public RunningRequestInfoBuilder nq(Long nq) {
            this.nq = nq;
            return this;
        }

        /**
         * Sets the top-k of the request.
         *
         * @param topK the topK
         * @return this builder
         */
        public RunningRequestInfoBuilder topK(Long topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Sets the leading part (at most 256 bytes) of the filter expression.
         *
         * @param expr the expr
         * @return this builder
         */
        public RunningRequestInfoBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Sets the time the request started, in milliseconds since the Unix epoch.
         *
         * @param startTimeMs the startTimeMs
         * @return this builder
         */
        public RunningRequestInfoBuilder startTimeMs(Long startTimeMs) {
            this.startTimeMs = startTimeMs;
            return this;
        }

        /**
         * Sets how long the request's first task waited in the proxy queue, in milliseconds; 0 until that
         * task starts.
         *
         * @param queuedMs the queuedMs
         * @return this builder
         */
        public RunningRequestInfoBuilder queuedMs(Long queuedMs) {
            this.queuedMs = queuedMs;
            return this;
        }

        /**
         * Sets how long the request has been running, in milliseconds. In a {@code cancelRequests} answer it
         * is how long the request had been running when it was canceled.
         *
         * @param elapsedMs the elapsedMs
         * @return this builder
         */
        public RunningRequestInfoBuilder elapsedMs(Long elapsedMs) {
            this.elapsedMs = elapsedMs;
            return this;
        }

        /**
         * Sets the request state: {@code Queued}, {@code Running} or {@code Canceling}. In a
         * {@code cancelRequests} answer it is the state before the cancel, or {@code Canceling} when the
         * call repeated an earlier cancel.
         *
         * @param state the state
         * @return this builder
         */
        public RunningRequestInfoBuilder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the ids of the proxy tasks the request has created.
         *
         * @param taskIds the taskIds
         * @return this builder
         */
        public RunningRequestInfoBuilder taskIds(List<Long> taskIds) {
            this.taskIds = taskIds;
            return this;
        }

        /**
         * Sets the trace id of the request.
         *
         * @param traceId the traceId
         * @return this builder
         */
        public RunningRequestInfoBuilder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        /**
         * Sets whether the request can be canceled.
         *
         * @param cancellable the cancellable
         * @return this builder
         */
        public RunningRequestInfoBuilder cancellable(Boolean cancellable) {
            this.cancellable = cancellable;
            return this;
        }

        /**
         * Builds the {@code RunningRequestInfo}.
         *
         * @return the constructed {@code RunningRequestInfo}
         */
        public RunningRequestInfo build() {
            return new RunningRequestInfo(this);
        }
    }
}
