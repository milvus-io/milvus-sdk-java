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

package io.milvus.bulkwriter.request.import_;

import java.util.ArrayList;
import java.util.List;

/**
 * Request for importing data into an open-source Milvus instance.
 *
 * <p>Imports data files stored in the bucket where Milvus resides, supporting single-file
 * or multi-path import through the files parameter.</p>
 */


public class MilvusImportRequest extends BaseImportRequest {
    private static final long serialVersionUID = -1958858397962018740L;
    /**
     * This parameter can be specified; defaults to the "default" database.
     */
    private String dbName;

    private String collectionName;

    /**
     * If the collection has partitionKey enabled:
     * - The partitionName parameter cannot be specified for import.
     * If the collection does not have partitionKey enabled:
     * - You may specify partitionName for the import.
     * - Defaults to the "default" partition if not specified.
     */
    private String partitionName;

    /**
     * Data import can be configured in multiple ways using `files`:
     * <p>
     * 1. Multi-path import (multiple files):
     * "files": [
     * ["parquet-folder-1/1.parquet"],
     * ["parquet-folder-2/1.parquet"],
     * ["parquet-folder-3/1.parquet"]
     * ]
     * <p>
     * 2. Single file import:
     * "files": [
     * ["parquet-folder/1.parquet"]
     * ]
     */
    private List<List<String>> files;
    /**
     * Creates a new MilvusImportRequest.
     */


    public MilvusImportRequest() {
    }
    /**
     * Creates a new MilvusImportRequest.
     *
     * @param dbName the dbName
     * @param collectionName the collectionName
     * @param partitionName the partitionName
     * @param files the files
     */


    public MilvusImportRequest(String dbName, String collectionName, String partitionName, List<List<String>> files) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.files = files;
    }

    protected MilvusImportRequest(MilvusImportRequestBuilder builder) {
        super(builder);
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.files = builder.files;
    }
    /**
     * Returns the dbName.
     *
     * @return the dbName
     */


    public String getDbName() {
        return dbName;
    }
    /**
     * Sets the dbName.
     *
     * @param dbName the dbName
     */


    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    /**
     * Returns the collectionName.
     *
     * @return the collectionName
     */


    public String getCollectionName() {
        return collectionName;
    }
    /**
     * Sets the collectionName.
     *
     * @param collectionName the collectionName
     */


    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }
    /**
     * Returns the partitionName.
     *
     * @return the partitionName
     */


    public String getPartitionName() {
        return partitionName;
    }
    /**
     * Sets the partitionName.
     *
     * @param partitionName the partitionName
     */


    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
    /**
     * Returns the files.
     *
     * @return the files
     */


    public List<List<String>> getFiles() {
        return files;
    }
    /**
     * Sets the files.
     *
     * @param files the files
     */


    public void setFiles(List<List<String>> files) {
        this.files = files;
    }

    @Override
    public String toString() {
        return "MilvusImportRequest{" +
                "dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", files=" + files +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static MilvusImportRequestBuilder builder() {
        return new MilvusImportRequestBuilder();
    }

    /**
     * Builder for {@link MilvusImportRequest} class.
     */


    public static class MilvusImportRequestBuilder extends BaseImportRequestBuilder<MilvusImportRequestBuilder> {
        private String dbName;
        private String collectionName;
        private String partitionName;
        private List<List<String>> files;

        private MilvusImportRequestBuilder() {
            this.dbName = "";
            this.collectionName = "";
            this.partitionName = "";
            this.files = new ArrayList<>();
        }
        /**
         * Sets the dbName.
         *
         * @param dbName the dbName
         * @return this builder
         */


        public MilvusImportRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }
        /**
         * Sets the collectionName.
         *
         * @param collectionName the collectionName
         * @return this builder
         */


        public MilvusImportRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }
        /**
         * Sets the partitionName.
         *
         * @param partitionName the partitionName
         * @return this builder
         */


        public MilvusImportRequestBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }
        /**
         * Sets the files.
         *
         * @param files the files
         * @return this builder
         */


        public MilvusImportRequestBuilder files(List<List<String>> files) {
            this.files = files;
            return this;
        }
        /**
         * Builds the MilvusImportRequest.
         *
         * @return the built MilvusImportRequest
         */


        public MilvusImportRequest build() {
            return new MilvusImportRequest(this);
        }
    }
}
