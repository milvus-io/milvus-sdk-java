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

package io.milvus.v2.service.rls.request;

/**
 * Request parameters for the {@code dropRowPolicy} API.
 */

public class DropRowPolicyReq {
    private String databaseName;
    private String collectionName;
    private String policyName;

    private DropRowPolicyReq(DropRowPolicyReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (builder.policyName == null || builder.policyName.isEmpty()) {
            throw new IllegalArgumentException("Policy name cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.policyName = builder.policyName;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */

    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
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
     * Returns the policy name.
     *
     * @return the policy name
     */

    public String getPolicyName() {
        return policyName;
    }

    /**
     * Sets the policy name.
     *
     * @param policyName the policy name
     */

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    @Override
    public String toString() {
        return "DropRowPolicyReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", policyName='" + policyName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DropRowPolicyReq}.
     *
     * @return a new {@link DropRowPolicyReqBuilder}
     */

    public static DropRowPolicyReqBuilder builder() {
        return new DropRowPolicyReqBuilder();
    }

    /**
     * Builder for {@link DropRowPolicyReq} class.
     */

    public static class DropRowPolicyReqBuilder {
        private String databaseName;
        private String collectionName;
        private String policyName;

        private DropRowPolicyReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public DropRowPolicyReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public DropRowPolicyReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the policy name.
         *
         * @param policyName the policy name
         * @return this builder
         */

        public DropRowPolicyReqBuilder policyName(String policyName) {
            this.policyName = policyName;
            return this;
        }

        /**
         * Builds the {@link DropRowPolicyReq}.
         *
         * @return the built request
         */

        public DropRowPolicyReq build() {
            return new DropRowPolicyReq(this);
        }
    }
}
