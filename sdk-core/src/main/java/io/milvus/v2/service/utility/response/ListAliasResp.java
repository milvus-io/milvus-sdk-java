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

import java.util.List;

/**
 * Response returned by the {@code listAliases} API.
 */
public class ListAliasResp {
    private String collectionName;
    private List<String> alias;
    private String dbName;

    private ListAliasResp(ListAliasRespBuilder builder) {
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
        this.dbName = builder.dbName;
    }

    public static ListAliasRespBuilder builder() {
        return new ListAliasRespBuilder();
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
     * Returns the aliases of the collection.
     *
     * @return the list of aliases
     */
    public List<String> getAlias() {
        return alias;
    }

    /**
     * Sets the aliases of the collection.
     *
     * @param alias the list of aliases
     */
    public void setAlias(List<String> alias) {
        this.alias = alias;
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

    @Override
    public String toString() {
        return "ListAliasResp{" +
                "collectionName='" + collectionName + '\'' +
                ", alias=" + alias +
                ", dbName='" + dbName + '\'' +
                '}';
    }

    public static class ListAliasRespBuilder {
        private String collectionName;
        private List<String> alias;
        private String dbName;

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public ListAliasRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the aliases of the collection.
         *
         * @param alias the list of aliases
         * @return this builder
         */
        public ListAliasRespBuilder alias(List<String> alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param dbName the database name
         * @return this builder
         */
        public ListAliasRespBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Builds the {@code ListAliasResp}.
         *
         * @return the constructed {@code ListAliasResp}
         */
        public ListAliasResp build() {
            return new ListAliasResp(this);
        }
    }
}
