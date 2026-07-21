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

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public CreateCollectionReq.Function getFunction() {
        return function;
    }

    public void setFunction(CreateCollectionReq.Function function) {
        this.function = function;
    }

    public IndexParam getIndexParam() {
        return indexParam;
    }

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

        public AddFunctionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AddFunctionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AddFunctionFieldReqBuilder function(CreateCollectionReq.Function function) {
            this.function = function;
            return this;
        }

        public AddFunctionFieldReqBuilder indexParam(IndexParam indexParam) {
            this.indexParam = indexParam;
            return this;
        }

        @Override
        public AddFunctionFieldReq build() {
            return new AddFunctionFieldReq(this);
        }
    }
}
