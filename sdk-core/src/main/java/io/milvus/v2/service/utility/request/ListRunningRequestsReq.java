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

/**
 * Request parameters for the {@code listRunningRequests} API.
 *
 * <p>The filters are combined with AND; an unset filter does not constrain the result.</p>
 */
public class ListRunningRequestsReq {
    private String databaseName;
    private String collectionName;
    private String user;
    private Long minElapsedMs;

    private ListRunningRequestsReq(ListRunningRequestsReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.user = builder.user;
        this.minElapsedMs = builder.minElapsedMs;
    }

    /**
     * Creates a new builder for {@code ListRunningRequestsReq}.
     *
     * @return the builder
     */
    public static ListRunningRequestsReqBuilder builder() {
        return new ListRunningRequestsReqBuilder();
    }

    /**
     * Returns the database name to match exactly; empty means any database.
     *
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name to match exactly; empty means any database.
     *
     * @param databaseName the databaseName
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name to match exactly; empty means any collection.
     *
     * @return the collectionName
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name to match exactly; empty means any collection.
     *
     * @param collectionName the collectionName
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the user whose requests to list; empty means every user. A caller without the
     * ListRunningRequests privilege may only see their own requests: empty then means the caller,
     * and any other name is denied.
     *
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user whose requests to list; empty means every user. A caller without the
     * ListRunningRequests privilege may only see their own requests: empty then means the caller,
     * and any other name is denied.
     *
     * @param user the user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Returns the minimum time, in milliseconds, a request has been running; 0 means no constraint.
     *
     * @return the minElapsedMs
     */
    public Long getMinElapsedMs() {
        return minElapsedMs;
    }

    /**
     * Sets the minimum time, in milliseconds, a request has been running; 0 means no constraint.
     *
     * @param minElapsedMs the minElapsedMs
     */
    public void setMinElapsedMs(Long minElapsedMs) {
        this.minElapsedMs = minElapsedMs;
    }

    @Override
    public String toString() {
        return "ListRunningRequestsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", user='" + user + '\'' +
                ", minElapsedMs=" + minElapsedMs +
                '}';
    }

    /**
     * Builder for {@link ListRunningRequestsReq}.
     */
    public static class ListRunningRequestsReqBuilder {
        private String databaseName = "";
        private String collectionName = "";
        private String user = "";
        private Long minElapsedMs = 0L;

        /**
         * Sets the database name to match exactly; empty means any database.
         *
         * @param databaseName the databaseName
         * @return this builder
         */
        public ListRunningRequestsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name to match exactly; empty means any collection.
         *
         * @param collectionName the collectionName
         * @return this builder
         */
        public ListRunningRequestsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the user whose requests to list; empty means every user. A caller without the
         * ListRunningRequests privilege may only see their own requests: empty then means the caller,
         * and any other name is denied.
         *
         * @param user the user
         * @return this builder
         */
        public ListRunningRequestsReqBuilder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * Sets the minimum time, in milliseconds, a request has been running; 0 means no constraint.
         *
         * @param minElapsedMs the minElapsedMs
         * @return this builder
         */
        public ListRunningRequestsReqBuilder minElapsedMs(Long minElapsedMs) {
            this.minElapsedMs = minElapsedMs;
            return this;
        }

        /**
         * Builds the {@code ListRunningRequestsReq}.
         *
         * @return the constructed {@code ListRunningRequestsReq}
         */
        public ListRunningRequestsReq build() {
            return new ListRunningRequestsReq(this);
        }
    }
}
