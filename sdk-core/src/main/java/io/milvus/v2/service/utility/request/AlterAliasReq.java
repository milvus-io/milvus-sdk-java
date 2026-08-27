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
 * Request parameters for the {@code alterAlias} API.
 */
public class AlterAliasReq {
    private String databaseName;
    private String collectionName;
    private String alias;

    private AlterAliasReq(AlterAliasReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
    }

    public static AlterAliasReqBuilder builder() {
        return new AlterAliasReqBuilder();
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
     * Returns the alias.
     *
     * @return the alias
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Sets the alias.
     *
     * @param alias the alias
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "AlterAliasReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }

    public static class AlterAliasReqBuilder {
        private String databaseName;
        private String collectionName;
        private String alias;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterAliasReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AlterAliasReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the alias.
         *
         * @param alias the alias
         * @return this builder
         */
        public AlterAliasReqBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Builds the {@code AlterAliasReq}.
         *
         * @return the constructed {@code AlterAliasReq}
         */
        public AlterAliasReq build() {
            return new AlterAliasReq(this);
        }
    }
}
