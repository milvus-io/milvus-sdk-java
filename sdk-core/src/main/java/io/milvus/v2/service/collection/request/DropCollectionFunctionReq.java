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

package io.milvus.v2.service.collection.request;

/**
 * Request parameters for the {@code dropCollectionFunction} API.
 */
public class DropCollectionFunctionReq {
    private String collectionName;
    private String databaseName;
    private String functionName;

    private DropCollectionFunctionReq(DropCollectionFunctionReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.functionName = builder.functionName;
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
     * Returns the name of the function to drop.
     *
     * @return the function name
     */
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public String toString() {
        return "DropCollectionFunctionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", functionName= '" + functionName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DropCollectionFunctionReq}.
     *
     * @return the builder
     */
    public static DropCollectionFunctionReqBuilder builder() {
        return new DropCollectionFunctionReqBuilder();
    }

    public static class DropCollectionFunctionReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private String functionName = "";

        private DropCollectionFunctionReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropCollectionFunctionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropCollectionFunctionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the name of the function to drop.
         *
         * @param functionName the function name
         * @return this builder
         */
        public DropCollectionFunctionReqBuilder functionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        /**
         * Builds a {@link DropCollectionFunctionReq} with the configured parameters.
         *
         * @return the request
         */
        public DropCollectionFunctionReq build() {
            return new DropCollectionFunctionReq(this);
        }
    }
}
