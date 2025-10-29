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

public class HasCollectionReq {
    private String databaseName;
    private String collectionName;

    private HasCollectionReq(HasCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        return "HasCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    public static HasCollectionReqBuilder builder() {
        return new HasCollectionReqBuilder();
    }

    public static class HasCollectionReqBuilder {
        private String databaseName;
        private String collectionName;

        private HasCollectionReqBuilder() {
        }

        public HasCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public HasCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public HasCollectionReq build() {
            return new HasCollectionReq(this);
        }
    }
}
