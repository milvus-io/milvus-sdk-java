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
 * Request parameters for the {@code listRLSPrincipals} API.
 */

public class ListRLSPrincipalsReq {
    private String databaseName;
    private String collectionName;

    private ListRLSPrincipalsReq(ListRLSPrincipalsReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
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

    @Override
    public String toString() {
        return "ListRLSPrincipalsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link ListRLSPrincipalsReq}.
     *
     * @return a new {@link ListRLSPrincipalsReqBuilder}
     */

    public static ListRLSPrincipalsReqBuilder builder() {
        return new ListRLSPrincipalsReqBuilder();
    }

    /**
     * Builder for {@link ListRLSPrincipalsReq} class.
     */

    public static class ListRLSPrincipalsReqBuilder {
        private String databaseName;
        private String collectionName;

        private ListRLSPrincipalsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public ListRLSPrincipalsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public ListRLSPrincipalsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Builds the {@link ListRLSPrincipalsReq}.
         *
         * @return the built request
         */

        public ListRLSPrincipalsReq build() {
            return new ListRLSPrincipalsReq(this);
        }
    }
}
