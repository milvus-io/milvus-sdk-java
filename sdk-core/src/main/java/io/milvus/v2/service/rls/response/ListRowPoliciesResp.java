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

package io.milvus.v2.service.rls.response;

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code listRowPolicies} API.
 */

public class ListRowPoliciesResp {
    private String dbName;
    private String collectionName;
    private List<RowPolicyInfo> policies;

    private ListRowPoliciesResp(ListRowPoliciesRespBuilder builder) {
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.policies = builder.policies;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */

    public String getDbName() {
        return dbName;
    }

    /**
     * Sets the database name.
     *
     * @param dbName the database name
     */

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */

    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the row policy definitions.
     *
     * @return the list of row policies
     */

    public List<RowPolicyInfo> getPolicies() {
        return policies;
    }

    /**
     * Sets the row policy definitions.
     *
     * @param policies the list of row policies
     */

    public void setPolicies(List<RowPolicyInfo> policies) {
        this.policies = policies;
    }

    @Override
    public String toString() {
        return "ListRowPoliciesResp{" +
                "dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", policies=" + policies +
                '}';
    }

    /**
     * Creates a new builder for {@link ListRowPoliciesResp}.
     *
     * @return a new {@link ListRowPoliciesRespBuilder}
     */

    public static ListRowPoliciesRespBuilder builder() {
        return new ListRowPoliciesRespBuilder();
    }

    /**
     * Builder for {@link ListRowPoliciesResp} class.
     */

    public static class ListRowPoliciesRespBuilder {
        private String dbName;
        private String collectionName;
        private List<RowPolicyInfo> policies = new ArrayList<>();

        private ListRowPoliciesRespBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param dbName the database name
         * @return this builder
         */

        public ListRowPoliciesRespBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public ListRowPoliciesRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the row policy definitions.
         *
         * @param policies the list of row policies
         * @return this builder
         */

        public ListRowPoliciesRespBuilder policies(List<RowPolicyInfo> policies) {
            this.policies = policies;
            return this;
        }

        /**
         * Builds the {@link ListRowPoliciesResp}.
         *
         * @return the built response
         */

        public ListRowPoliciesResp build() {
            return new ListRowPoliciesResp(this);
        }
    }
}
