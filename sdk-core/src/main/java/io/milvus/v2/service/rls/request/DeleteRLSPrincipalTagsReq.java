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

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code deleteRLSPrincipalTags} API.
 */

public class DeleteRLSPrincipalTagsReq {
    private String databaseName;
    private String collectionName;
    private String principalName;
    private List<String> tagKeys;

    private DeleteRLSPrincipalTagsReq(DeleteRLSPrincipalTagsReqBuilder builder) {
        if (builder.collectionName == null || builder.collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (builder.principalName == null || builder.principalName.isEmpty()) {
            throw new IllegalArgumentException("Principal name cannot be null or empty");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.principalName = builder.principalName;
        this.tagKeys = builder.tagKeys;
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
     * Returns the tag keys to delete. An empty or {@code null} list deletes all tags.
     *
     * @return the tag keys
     */

    public List<String> getTagKeys() {
        return tagKeys;
    }

    /**
     * Sets the tag keys to delete. An empty or {@code null} list deletes all tags.
     *
     * @param tagKeys the tag keys
     */

    public void setTagKeys(List<String> tagKeys) {
        this.tagKeys = tagKeys;
    }

    @Override
    public String toString() {
        return "DeleteRLSPrincipalTagsReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", principalName='" + principalName + '\'' +
                ", tagKeys=" + tagKeys +
                '}';
    }

    /**
     * Creates a new builder for {@link DeleteRLSPrincipalTagsReq}.
     *
     * @return a new {@link DeleteRLSPrincipalTagsReqBuilder}
     */

    public static DeleteRLSPrincipalTagsReqBuilder builder() {
        return new DeleteRLSPrincipalTagsReqBuilder();
    }

    /**
     * Builder for {@link DeleteRLSPrincipalTagsReq} class.
     */

    public static class DeleteRLSPrincipalTagsReqBuilder {
        private String databaseName;
        private String collectionName;
        private String principalName;
        private List<String> tagKeys = new ArrayList<>();

        private DeleteRLSPrincipalTagsReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */

        public DeleteRLSPrincipalTagsReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */

        public DeleteRLSPrincipalTagsReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the principal name.
         *
         * @param principalName the principal name
         * @return this builder
         */

        public DeleteRLSPrincipalTagsReqBuilder principalName(String principalName) {
            this.principalName = principalName;
            return this;
        }

        /**
         * Sets the tag keys to delete. An empty or {@code null} list deletes all tags.
         *
         * @param tagKeys the tag keys
         * @return this builder
         */

        public DeleteRLSPrincipalTagsReqBuilder tagKeys(List<String> tagKeys) {
            this.tagKeys = tagKeys;
            return this;
        }

        /**
         * Builds the {@link DeleteRLSPrincipalTagsReq}.
         *
         * @return the built request
         */

        public DeleteRLSPrincipalTagsReq build() {
            return new DeleteRLSPrincipalTagsReq(this);
        }
    }
}
