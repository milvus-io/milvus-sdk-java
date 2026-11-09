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

import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.common.RowPolicyType;

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code createRowPolicy} API.
 */

public class CreateRowPolicyReq {
    private String databaseName;
    private String collectionName;
    private String policyName;
    private RowPolicyType policyType;
    private List<RowPolicyAction> actions;
    private String usingExpr;
    private String checkExpr;
    private String description;

    private CreateRowPolicyReq(CreateRowPolicyReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (builder.policyName == null || builder.policyName.isEmpty()) {
            throw new IllegalArgumentException("Policy name cannot be null or empty");
        }
        if (builder.policyType == null) {
            throw new IllegalArgumentException("Policy type cannot be null");
        }
        if (builder.actions == null || builder.actions.isEmpty()) {
            throw new IllegalArgumentException("Actions cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.policyName = builder.policyName;
        this.policyType = builder.policyType;
        this.actions = builder.actions;
        this.usingExpr = builder.usingExpr;
        this.checkExpr = builder.checkExpr;
        this.description = builder.description;
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

    /**
     * Returns the policy type.
     *
     * @return the policy type
     */

    public RowPolicyType getPolicyType() {
        return policyType;
    }

    /**
     * Sets the policy type.
     *
     * @param policyType the policy type
     */

    public void setPolicyType(RowPolicyType policyType) {
        this.policyType = policyType;
    }

    /**
     * Returns the actions governed by the policy.
     *
     * @return the list of actions
     */

    public List<RowPolicyAction> getActions() {
        return actions;
    }

    /**
     * Sets the actions governed by the policy.
     *
     * @param actions the list of actions
     */

    public void setActions(List<RowPolicyAction> actions) {
        this.actions = actions;
    }

    /**
     * Returns the expression used to filter rows visible to a principal.
     *
     * @return the using expression
     */

    public String getUsingExpr() {
        return usingExpr;
    }

    /**
     * Sets the expression used to filter rows visible to a principal.
     *
     * @param usingExpr the using expression
     */

    public void setUsingExpr(String usingExpr) {
        this.usingExpr = usingExpr;
    }

    /**
     * Returns the expression used to validate rows written by a principal.
     *
     * @return the check expression
     */

    public String getCheckExpr() {
        return checkExpr;
    }

    /**
     * Sets the expression used to validate rows written by a principal.
     *
     * @param checkExpr the check expression
     */

    public void setCheckExpr(String checkExpr) {
        this.checkExpr = checkExpr;
    }

    /**
     * Returns the policy description.
     *
     * @return the policy description
     */

    public String getDescription() {
        return description;
    }

    /**
     * Sets the policy description.
     *
     * @param description the policy description
     */

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "CreateRowPolicyReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", policyName='" + policyName + '\'' +
                ", policyType=" + policyType +
                ", actions=" + actions +
                ", usingExpr='" + usingExpr + '\'' +
                ", checkExpr='" + checkExpr + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link CreateRowPolicyReq}.
     *
     * @return a new {@link CreateRowPolicyReqBuilder}
     */

    public static CreateRowPolicyReqBuilder builder() {
        return new CreateRowPolicyReqBuilder();
    }

    /**
     * Builder for {@link CreateRowPolicyReq} class.
     */

    public static class CreateRowPolicyReqBuilder {
        private String databaseName;
        private String collectionName;
        private String policyName;
        private RowPolicyType policyType;
        private List<RowPolicyAction> actions = new ArrayList<>();
        private String usingExpr = "";
        private String checkExpr = "";
        private String description = "";

        private CreateRowPolicyReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public CreateRowPolicyReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public CreateRowPolicyReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the policy name.
         *
         * @param policyName the policy name
         * @return this builder
         */

        public CreateRowPolicyReqBuilder policyName(String policyName) {
            this.policyName = policyName;
            return this;
        }

        /**
         * Sets the policy type.
         *
         * @param policyType the policy type
         * @return this builder
         */

        public CreateRowPolicyReqBuilder policyType(RowPolicyType policyType) {
            this.policyType = policyType;
            return this;
        }

        /**
         * Sets the actions governed by the policy.
         *
         * @param actions the list of actions
         * @return this builder
         */

        public CreateRowPolicyReqBuilder actions(List<RowPolicyAction> actions) {
            this.actions = actions;
            return this;
        }

        /**
         * Sets the expression used to filter rows visible to a principal.
         *
         * @param usingExpr the using expression
         * @return this builder
         */

        public CreateRowPolicyReqBuilder usingExpr(String usingExpr) {
            this.usingExpr = usingExpr;
            return this;
        }

        /**
         * Sets the expression used to validate rows written by a principal.
         *
         * @param checkExpr the check expression
         * @return this builder
         */

        public CreateRowPolicyReqBuilder checkExpr(String checkExpr) {
            this.checkExpr = checkExpr;
            return this;
        }

        /**
         * Sets the policy description.
         *
         * @param description the policy description
         * @return this builder
         */

        public CreateRowPolicyReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link CreateRowPolicyReq}.
         *
         * @return the built request
         */

        public CreateRowPolicyReq build() {
            return new CreateRowPolicyReq(this);
        }
    }
}
