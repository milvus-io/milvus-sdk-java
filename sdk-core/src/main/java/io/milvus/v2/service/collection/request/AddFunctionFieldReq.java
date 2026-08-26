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

import io.milvus.v2.common.IndexParam;

/**
 * Request parameters for the {@code addFunctionField} API, which adds a function
 * and its output field together with an optional index on the output field.
 */
public class AddFunctionFieldReq extends AddFieldReq {
    private String collectionName;
    private String databaseName;
    private CreateCollectionReq.Function function;
    private IndexParam indexParam;

    private AddFunctionFieldReq(AddFunctionFieldReqBuilder builder) {
        super(builder);
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.function = builder.function;
        this.indexParam = builder.indexParam;
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
     * Returns the function to add to the collection.
     *
     * @return the function
     */
    public CreateCollectionReq.Function getFunction() {
        return function;
    }

    /**
     * Sets the function to add to the collection.
     *
     * @param function the function
     */
    public void setFunction(CreateCollectionReq.Function function) {
        this.function = function;
    }

    /**
     * Returns the index parameters for the function output field.
     *
     * @return the index parameters
     */
    public IndexParam getIndexParam() {
        return indexParam;
    }

    /**
     * Sets the index parameters for the function output field.
     *
     * @param indexParam the index parameters
     */
    public void setIndexParam(IndexParam indexParam) {
        this.indexParam = indexParam;
    }

    @Override
    public String toString() {
        return "AddFunctionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", function=" + function +
                ", indexParam=" + indexParam +
                ", " + super.toString() +
                '}';
    }

    /**
     * Creates a new builder for {@link AddFunctionFieldReq}.
     *
     * @return the builder
     */
    public static AddFunctionFieldReqBuilder builder() {
        return new AddFunctionFieldReqBuilder();
    }

    public static class AddFunctionFieldReqBuilder extends AddFieldReq.AddFieldReqBuilder<AddFunctionFieldReqBuilder> {
        private String collectionName = "";
        private String databaseName = "";
        private CreateCollectionReq.Function function;
        private IndexParam indexParam;

        private AddFunctionFieldReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AddFunctionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AddFunctionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the function to add to the collection.
         *
         * @param function the function
         * @return this builder
         */
        public AddFunctionFieldReqBuilder function(CreateCollectionReq.Function function) {
            this.function = function;
            return this;
        }

        /**
         * Sets the index parameters for the function output field.
         *
         * @param indexParam the index parameters
         * @return this builder
         */
        public AddFunctionFieldReqBuilder indexParam(IndexParam indexParam) {
            this.indexParam = indexParam;
            return this;
        }

        /**
         * Builds an {@link AddFunctionFieldReq} with the configured parameters.
         *
         * @return the request
         */
        @Override
        public AddFunctionFieldReq build() {
            return new AddFunctionFieldReq(this);
        }
    }
}
