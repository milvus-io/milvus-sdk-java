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

package io.milvus.bulkwriter.request.list;

public class MilvusListImportJobsRequest extends BaseListImportJobsRequest {
    private static final long serialVersionUID = 8957739122547766268L;
    private String collectionName;
    // this parameter "dbName" will be converted to JSON and passed to server
    // milvus http server requires "dbName", not "databaseName"
    private String dbName;

    protected MilvusListImportJobsRequest() {
    }

    protected MilvusListImportJobsRequest(String collectionName) {
        this.collectionName = collectionName;
    }

    protected MilvusListImportJobsRequest(MilvusListImportJobsRequestBuilder builder) {
        super(builder);
        this.collectionName = builder.collectionName;
        this.dbName = builder.dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        return "MilvusListImportJobsRequest{" +
                "collectionName='" + collectionName + '\'' +
                "dbName='" + dbName + '\'' +
                '}';
    }

    public static MilvusListImportJobsRequestBuilder builder() {
        return new MilvusListImportJobsRequestBuilder();
    }

    public static class MilvusListImportJobsRequestBuilder extends BaseListImportJobsRequestBuilder<MilvusListImportJobsRequestBuilder> {
        private String collectionName;
        private String dbName;

        private MilvusListImportJobsRequestBuilder() {
            this.collectionName = "";
            this.dbName = "";
        }

        public MilvusListImportJobsRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public MilvusListImportJobsRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public MilvusListImportJobsRequest build() {
            return new MilvusListImportJobsRequest(this);
        }
    }
}
