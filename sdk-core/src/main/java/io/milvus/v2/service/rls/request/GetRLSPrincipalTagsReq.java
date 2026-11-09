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
 * Request parameters for the {@code getRLSPrincipalTags} API.
 */

public class GetRLSPrincipalTagsReq {
    private String databaseName;
    private String collectionName;
    private String principalName;

    private GetRLSPrincipalTagsReq(GetRLSPrincipalTagsReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (builder.principalName == null || builder.principalName.isEmpty()) {
            throw new IllegalArgumentException("Principal name cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.principalName = builder.principalName;
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
     * Returns the principal name.
     *
     * @return the principal name
     */

    public String getPrincipalName() {
        return principalName;
    }

    /**
     * Sets the principal name.
     *
     * @param principalName the principal name
     */

    public void setPrincipalName(String principalName) {
        this.principalName = principalName;
    }

    @Override
    public String toString() {
        return "GetRLSPrincipalTagsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", principalName='" + principalName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link GetRLSPrincipalTagsReq}.
     *
     * @return a new {@link GetRLSPrincipalTagsReqBuilder}
     */

    public static GetRLSPrincipalTagsReqBuilder builder() {
        return new GetRLSPrincipalTagsReqBuilder();
    }

    /**
     * Builder for {@link GetRLSPrincipalTagsReq} class.
     */

    public static class GetRLSPrincipalTagsReqBuilder {
        private String databaseName;
        private String collectionName;
        private String principalName;

        private GetRLSPrincipalTagsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public GetRLSPrincipalTagsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public GetRLSPrincipalTagsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the principal name.
         *
         * @param principalName the principal name
         * @return this builder
         */

        public GetRLSPrincipalTagsReqBuilder principalName(String principalName) {
            this.principalName = principalName;
            return this;
        }

        /**
         * Builds the {@link GetRLSPrincipalTagsReq}.
         *
         * @return the built request
         */

        public GetRLSPrincipalTagsReq build() {
            return new GetRLSPrincipalTagsReq(this);
        }
    }
}
