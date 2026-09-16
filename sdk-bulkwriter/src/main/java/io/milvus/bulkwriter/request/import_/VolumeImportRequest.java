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
 * Request for importing data from a Zilliz volume into a Zilliz cloud instance.
 *
 * <p>Supports multi-path, folder, or single-file import through data paths within the
 * specified volume.</p>
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
    /**
     * Creates a new VolumeImportRequest.
     */


    public VolumeImportRequest() {
    }
    /**
     * Creates a new VolumeImportRequest.
     *
     * @param clusterId the clusterId
     * @param dbName the dbName
     * @param collectionName the collectionName
     * @param partitionName the partitionName
     * @param volumeName the volumeName
     * @param dataPaths the dataPaths
     */


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
    /**
     * Returns the clusterId.
     *
     * @return the clusterId
     */


    public String getClusterId() {
        return clusterId;
    }
    /**
     * Sets the clusterId.
     *
     * @param clusterId the clusterId
     */


    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
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
     * Returns the volumeName.
     *
     * @return the volumeName
     */


    public String getVolumeName() {
        return volumeName;
    }
    /**
     * Sets the volumeName.
     *
     * @param volumeName the volumeName
     */


    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }
    /**
     * Returns the dataPaths.
     *
     * @return the dataPaths
     */


    public List<List<String>> getDataPaths() {
        return dataPaths;
    }
    /**
     * Sets the dataPaths.
     *
     * @param dataPaths the dataPaths
     */


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
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static VolumeImportRequestBuilder builder() {
        return new VolumeImportRequestBuilder();
    }

    /**
     * Builder for {@link VolumeImportRequest} class.
     */


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
        /**
         * Sets the clusterId.
         *
         * @param clusterId the clusterId
         * @return this builder
         */


        public VolumeImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }
        /**
         * Sets the dbName.
         *
         * @param dbName the dbName
         * @return this builder
         */


        public VolumeImportRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }
        /**
         * Sets the collectionName.
         *
         * @param collectionName the collectionName
         * @return this builder
         */


        public VolumeImportRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }
        /**
         * Sets the partitionName.
         *
         * @param partitionName the partitionName
         * @return this builder
         */


        public VolumeImportRequestBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }
        /**
         * Sets the volumeName.
         *
         * @param volumeName the volumeName
         * @return this builder
         */


        public VolumeImportRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }
        /**
         * Sets the dataPaths.
         *
         * @param dataPaths the dataPaths
         * @return this builder
         */


        public VolumeImportRequestBuilder dataPaths(List<List<String>> dataPaths) {
            this.dataPaths = dataPaths;
            return this;
        }
        /**
         * Builds the VolumeImportRequest.
         *
         * @return the built VolumeImportRequest
         */


        public VolumeImportRequest build() {
            return new VolumeImportRequest(this);
        }
    }
}
