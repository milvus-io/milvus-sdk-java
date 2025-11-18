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
  If you want to import data into a Zilliz cloud instance and your data is stored in a Zilliz volume,
  you can use this method to import the data from the volume.
 */
public class VolumeImportRequest extends BaseImportRequest {
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

    private String volumeName;

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

    public VolumeImportRequest() {
    }

    public VolumeImportRequest(String clusterId, String dbName, String collectionName, String partitionName,
                               String volumeName, List<List<String>> dataPaths) {
        this.clusterId = clusterId;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.volumeName = volumeName;
        this.dataPaths = dataPaths;
    }

    protected VolumeImportRequest(VolumeImportRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.volumeName = builder.volumeName;
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

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public List<List<String>> getDataPaths() {
        return dataPaths;
    }

    public void setDataPaths(List<List<String>> dataPaths) {
        this.dataPaths = dataPaths;
    }

    @Override
    public String toString() {
        return "VolumeImportRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", volumeName='" + volumeName + '\'' +
                ", dataPaths=" + dataPaths +
                '}';
    }

    public static VolumeImportRequestBuilder builder() {
        return new VolumeImportRequestBuilder();
    }

    public static class VolumeImportRequestBuilder extends BaseImportRequestBuilder<VolumeImportRequestBuilder> {
        private String clusterId;
        private String dbName;
        private String collectionName;
        private String partitionName;
        private String volumeName;
        private List<List<String>> dataPaths;

        private VolumeImportRequestBuilder() {
            this.clusterId = "";
            this.dbName = "";
            this.collectionName = "";
            this.partitionName = "";
            this.volumeName = "";
            this.dataPaths = new ArrayList<>();
        }

        public VolumeImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public VolumeImportRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public VolumeImportRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public VolumeImportRequestBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public VolumeImportRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public VolumeImportRequestBuilder dataPaths(List<List<String>> dataPaths) {
            this.dataPaths = dataPaths;
            return this;
        }

        public VolumeImportRequest build() {
            return new VolumeImportRequest(this);
        }
    }
}
