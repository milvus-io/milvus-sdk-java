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

public class AlterCollectionFunctionReq {
    private String collectionName;
    private String databaseName;
    private CreateCollectionReq.Function function;

    private AlterCollectionFunctionReq(AlterCollectionFunctionReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.function = builder.function;
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

    @Override
    public String toString() {
        return "AlterCollectionFunctionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", function= " + function +
                '}';
    }

    public static AlterCollectionFunctionReqBuilder builder() {
        return new AlterCollectionFunctionReqBuilder();
    }

    public static class AlterCollectionFunctionReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private CreateCollectionReq.Function function;

        private AlterCollectionFunctionReqBuilder() {
        }

        public AlterCollectionFunctionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AlterCollectionFunctionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AlterCollectionFunctionReqBuilder function(CreateCollectionReq.Function function) {
            this.function = function;
            return this;
        }

        public AlterCollectionFunctionReq build() {
            return new AlterCollectionFunctionReq(this);
        }
    }
}
