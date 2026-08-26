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

/**
 * Request parameters for listing bulk import jobs on a Milvus server.
 *
 * <p>It filters the import jobs by collection name and database name, and inherits the
 * API key handling from {@link BaseListImportJobsRequest}.</p>
 */
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

    /**
     * Returns the name of the collection whose import jobs are listed.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection whose import jobs are listed.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the name of the database whose import jobs are listed.
     *
     * @return the database name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Sets the name of the database whose import jobs are listed.
     *
     * @param dbName the database name
     */
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

    /**
     * Returns a new builder for a {@link MilvusListImportJobsRequest}.
     *
     * @return a {@code MilvusListImportJobsRequest} builder
     */
    public static MilvusListImportJobsRequestBuilder builder() {
        return new MilvusListImportJobsRequestBuilder();
    }

    /**
     * Builder for {@link MilvusListImportJobsRequest}.
     */
    public static class MilvusListImportJobsRequestBuilder extends BaseListImportJobsRequestBuilder<MilvusListImportJobsRequestBuilder> {
        private String collectionName;
        private String dbName;

        private MilvusListImportJobsRequestBuilder() {
            this.collectionName = "";
            this.dbName = "";
        }

        /**
         * Sets the name of the collection whose import jobs are listed.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public MilvusListImportJobsRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the database whose import jobs are listed.
         *
         * @param dbName the database name
         * @return this builder
         */
        public MilvusListImportJobsRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Builds the {@link MilvusListImportJobsRequest} instance.
         *
         * @return the built {@code MilvusListImportJobsRequest}
         */
        public MilvusListImportJobsRequest build() {
            return new MilvusListImportJobsRequest(this);
        }
    }
}
