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

/*
  If you want to import data into a Zilliz cloud instance and your data is stored in a Zilliz stage,
  you can use this method to import the data from the stage.
 */
public class StageImportRequest extends BaseImportRequest {
    private String clusterId;

    /**
     * For Free & Serverless deployments: specifying this parameter is not supported.
     * For Dedicated deployments: this parameter can be specified; defaults to the "default" database.
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

    private String stageName;

    /**
     * Data import can be configured in multiple ways using `dataPaths`:
     * <p>
     * 1. Multi-path import (multiple folders or files):
     * "dataPaths": [
     * ["parquet-folder-1/1.parquet"],
     * ["parquet-folder-2/1.parquet"],
     * ["parquet-folder-3/"]
     * ]
     * <p>
     * 2. Folder import:
     * "dataPaths": [
     * ["parquet-folder/"]
     * ]
     * <p>
     * 3. Single file import:
     * "dataPaths": [
     * ["parquet-folder/1.parquet"]
     * ]
     */
    private List<List<String>> dataPaths;

    public StageImportRequest() {
    }

    public StageImportRequest(String clusterId, String dbName, String collectionName, String partitionName,
                              String stageName, List<List<String>> dataPaths) {
        this.clusterId = clusterId;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.stageName = stageName;
        this.dataPaths = dataPaths;
    }

    protected StageImportRequest(StageImportRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.stageName = builder.stageName;
        this.dataPaths = builder.dataPaths;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public List<List<String>> getDataPaths() {
        return dataPaths;
    }

    public void setDataPaths(List<List<String>> dataPaths) {
        this.dataPaths = dataPaths;
    }

    @Override
    public String toString() {
        return "StageImportRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", stageName='" + stageName + '\'' +
                ", dataPaths=" + dataPaths +
                '}';
    }

    public static StageImportRequestBuilder builder() {
        return new StageImportRequestBuilder();
    }

    public static class StageImportRequestBuilder extends BaseImportRequestBuilder<StageImportRequestBuilder> {
        private String clusterId;
        private String dbName;
        private String collectionName;
        private String partitionName;
        private String stageName;
        private List<List<String>> dataPaths;

        private StageImportRequestBuilder() {
            this.clusterId = "";
            this.dbName = "";
            this.collectionName = "";
            this.partitionName = "";
            this.stageName = "";
            this.dataPaths = new ArrayList<>();
        }

        public StageImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public StageImportRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public StageImportRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public StageImportRequestBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public StageImportRequestBuilder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public StageImportRequestBuilder dataPaths(List<List<String>> dataPaths) {
            this.dataPaths = dataPaths;
            return this;
        }

        public StageImportRequest build() {
            return new StageImportRequest(this);
        }
    }
}
