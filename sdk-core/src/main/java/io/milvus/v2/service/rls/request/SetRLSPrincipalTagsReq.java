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

import java.util.HashMap;
import java.util.Map;

/**
 * Request parameters for the {@code setRLSPrincipalTags} API.
 */

public class SetRLSPrincipalTagsReq {
    private String databaseName;
    private String collectionName;
    private String principalName;
    private Map<String, Object> tags;

    private SetRLSPrincipalTagsReq(SetRLSPrincipalTagsReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (builder.principalName == null || builder.principalName.isEmpty()) {
            throw new IllegalArgumentException("Principal name cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.principalName = builder.principalName;
        this.tags = builder.tags;
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

    /**
     * Returns the principal tags. Values may be strings, finite numbers, or
     * one-dimensional arrays of strings or numbers.
     *
     * @return the principal tags
     */

    public Map<String, Object> getTags() {
        return tags;
    }

    /**
     * Sets the principal tags. Values may be strings, finite numbers, or
     * one-dimensional arrays of strings or numbers.
     *
     * @param tags the principal tags
     */

    public void setTags(Map<String, Object> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "SetRLSPrincipalTagsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", principalName='" + principalName + '\'' +
                ", tags=" + tags +
                '}';
    }

    /**
     * Creates a new builder for {@link SetRLSPrincipalTagsReq}.
     *
     * @return a new {@link SetRLSPrincipalTagsReqBuilder}
     */

    public static SetRLSPrincipalTagsReqBuilder builder() {
        return new SetRLSPrincipalTagsReqBuilder();
    }

    /**
     * Builder for {@link SetRLSPrincipalTagsReq} class.
     */

    public static class SetRLSPrincipalTagsReqBuilder {
        private String databaseName;
        private String collectionName;
        private String principalName;
        private Map<String, Object> tags = new HashMap<>();

        private SetRLSPrincipalTagsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public SetRLSPrincipalTagsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public SetRLSPrincipalTagsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the principal name.
         *
         * @param principalName the principal name
         * @return this builder
         */

        public SetRLSPrincipalTagsReqBuilder principalName(String principalName) {
            this.principalName = principalName;
            return this;
        }

        /**
         * Sets the principal tags. Values may be strings, finite numbers, or
         * one-dimensional arrays of strings or numbers.
         *
         * @param tags the principal tags
         * @return this builder
         */

        public SetRLSPrincipalTagsReqBuilder tags(Map<String, Object> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Builds the {@link SetRLSPrincipalTagsReq}.
         *
         * @return the built request
         */

        public SetRLSPrincipalTagsReq build() {
            return new SetRLSPrincipalTagsReq(this);
        }
    }
}
