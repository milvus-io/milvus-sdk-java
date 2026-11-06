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

package io.milvus.v2.service.utility.request;

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code cancelRequests} API.
 */
public class CancelRequestsReq {
    private List<String> requestIds;
    private String reason;
    private String user;

    private CancelRequestsReq(CancelRequestsReqBuilder builder) {
        this.requestIds = builder.requestIds;
        this.reason = builder.reason;
        this.user = builder.user;
    }

    /**
     * Creates a new builder for {@code CancelRequestsReq}.
     *
     * @return the builder
     */
    public static CancelRequestsReqBuilder builder() {
        return new CancelRequestsReqBuilder();
    }

    /**
     * Returns the ids of the requests to cancel, exactly as returned by {@code listRunningRequests}; must not be
     * empty.
     *
     * @return the requestIds
     */
    public List<String> getRequestIds() {
        return requestIds;
    }

    /**
     * Sets the ids of the requests to cancel, exactly as returned by {@code listRunningRequests}; must not be
     * empty.
     *
     * @param requestIds the requestIds
     */
    public void setRequestIds(List<String> requestIds) {
        this.requestIds = requestIds;
    }

    /**
     * Returns a free-text reason, recorded in the server's audit log and returned to the client whose request
     * is canceled.
     *
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Sets a free-text reason, recorded in the server's audit log and returned to the client whose request
     * is canceled.
     *
     * @param reason the reason
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * Returns when set, only requests issued by this user are canceled; the ids of other users' requests are
     * reported as not found. A caller without the CancelRequests privilege may only cancel their own
     * requests: empty then means the caller, and any other name is denied.
     *
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets when set, only requests issued by this user are canceled; the ids of other users' requests are
     * reported as not found. A caller without the CancelRequests privilege may only cancel their own
     * requests: empty then means the caller, and any other name is denied.
     *
     * @param user the user
     */
    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "CancelRequestsReq{" +
                "requestIds=" + requestIds +
                ", reason='" + reason + '\'' +
                ", user='" + user + '\'' +
                '}';
    }

    /**
     * Builder for {@link CancelRequestsReq}.
     */
    public static class CancelRequestsReqBuilder {
        private List<String> requestIds = new ArrayList<>();
        private String reason = "";
        private String user = "";

        /**
         * Sets the ids of the requests to cancel, exactly as returned by {@code listRunningRequests}; must not be
         * empty.
         *
         * @param requestIds the requestIds
         * @return this builder
         */
        public CancelRequestsReqBuilder requestIds(List<String> requestIds) {
            this.requestIds = requestIds;
            return this;
        }

        /**
         * Sets a free-text reason, recorded in the server's audit log and returned to the client whose request
         * is canceled.
         *
         * @param reason the reason
         * @return this builder
         */
        public CancelRequestsReqBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Sets when set, only requests issued by this user are canceled; the ids of other users' requests are
         * reported as not found. A caller without the CancelRequests privilege may only cancel their own
         * requests: empty then means the caller, and any other name is denied.
         *
         * @param user the user
         * @return this builder
         */
        public CancelRequestsReqBuilder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * Builds the {@code CancelRequestsReq}.
         *
         * @return the constructed {@code CancelRequestsReq}
         */
        public CancelRequestsReq build() {
            return new CancelRequestsReq(this);
        }
    }
}
