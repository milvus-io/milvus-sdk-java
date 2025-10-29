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

public class AddCollectionFieldReq extends AddFieldReq {
    private String collectionName;
    private String databaseName;

    private AddCollectionFieldReq(AddCollectionFieldReqBuilder builder) {
        super(builder);
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
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

    @Override
    public String toString() {
        return "AddCollectionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", " + super.toString() +
                '}';
    }

    public static AddCollectionFieldReqBuilder builder() {
        return new AddCollectionFieldReqBuilder();
    }

    public static class AddCollectionFieldReqBuilder extends AddFieldReq.AddFieldReqBuilder<AddCollectionFieldReqBuilder> {
        private String collectionName;
        private String databaseName;

        private AddCollectionFieldReqBuilder() {
        }

        public AddCollectionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AddCollectionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        @Override
        public AddCollectionFieldReq build() {
            return new AddCollectionFieldReq(this);
        }
    }
}
