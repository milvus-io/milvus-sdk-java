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
 * Request parameters for the {@code listCompactionTasks} API.
 */


public class ListCompactionTasksReq {
    private String collectionName;
    private String databaseName;

    private ListCompactionTasksReq(ListCompactionTasksReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
    }

    /**
     * Creates a new builder for {@code ListCompactionTasksReq}.
     *
     * @return the builder
     */


    public static ListCompactionTasksReqBuilder builder() {
        return new ListCompactionTasksReqBuilder();
    }

    /**
     * Returns the name of the collection whose retained compaction tasks are to be listed.
     *
     * @return the collection name
     */


    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection whose retained compaction tasks are to be listed.
     *
     * @param collectionName the collection name
     */


    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the name of the database holding the collection.
     *
     * @return the database name
     */


    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the database holding the collection.
     *
     * @param databaseName the database name
     */


    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String toString() {
        return "ListCompactionTasksReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }

    /**
     * Builder for {@link ListCompactionTasksReq} class.
     */


    public static class ListCompactionTasksReqBuilder {
        private String collectionName;
        private String databaseName;

        /**
         * Sets the name of the collection whose retained compaction tasks are to be listed.
         *
         * @param collectionName the collection name
         * @return this builder
         */


        public ListCompactionTasksReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the database holding the collection.
         *
         * @param databaseName the database name
         * @return this builder
         */


        public ListCompactionTasksReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Builds the {@code ListCompactionTasksReq}.
         *
         * @return the constructed {@code ListCompactionTasksReq}
         */


        public ListCompactionTasksReq build() {
            return new ListCompactionTasksReq(this);
        }
    }
}
