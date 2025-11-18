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
  If you want to import data into a Zilliz cloud instance and your data is stored in a storage bucket,
  you can use this method to import the data from the bucket.
 */
public class CloudImportRequest extends BaseImportRequest {
    private static final long serialVersionUID = 6487348610099924813L;
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

    /**
     * Data import can be configured in multiple ways using `objectUrls`:
     * <p>
     * 1. Multi-path import (multiple folders or files):
     * "objectUrls": [
     * ["s3://bucket-name/parquet-folder-1/1.parquet"],
     * ["s3://bucket-name/parquet-folder-2/1.parquet"],
     * ["s3://bucket-name/parquet-folder-3/"]
     * ]
     * <p>
     * 2. Folder import:
     * "objectUrls": [
     * ["s3://bucket-name/parquet-folder/"]
     * ]
     * <p>
     * 3. Single file import:
     * "objectUrls": [
     * ["s3://bucket-name/parquet-folder/1.parquet"]
     * ]
     */
    private List<List<String>> objectUrls;

    /**
     * Use `objectUrls` instead for more flexible multi-path configuration.
     * <p>
     * Folder import:
     * "objectUrl": "s3://bucket-name/parquet-folder/"
     * <p>
     * File import:
     * "objectUrl": "s3://bucket-name/parquet-folder/1.parquet"
     */
    @Deprecated
    private String objectUrl;

    /**
     * Specify `accessKey` and `secretKey`; for short-term credentials, also include `token`.
     */
    private String accessKey;

    /**
     * Specify `accessKey` and `secretKey`; for short-term credentials, also include `token`.
     */
    private String secretKey;

    /**
     * Specify `accessKey` and `secretKey`; for short-term credentials, also include `token`.
     */
    private String token;

    public CloudImportRequest() {
    }

    public CloudImportRequest(String clusterId, String dbName, String collectionName, String partitionName,
                              List<List<String>> objectUrls, String accessKey, String secretKey, String token) {
        this.clusterId = clusterId;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.objectUrls = objectUrls;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.token = token;
    }

    protected CloudImportRequest(CloudImportRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.objectUrls = builder.objectUrls;
        this.objectUrl = builder.objectUrl;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.token = builder.token;
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

    public List<List<String>> getObjectUrls() {
        return objectUrls;
    }

    public void setObjectUrls(List<List<String>> objectUrls) {
        this.objectUrls = objectUrls;
    }

    public String getObjectUrl() {
        return objectUrl;
    }

    public void setObjectUrl(String objectUrl) {
        this.objectUrl = objectUrl;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "CloudImportRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", objectUrls=" + objectUrls +
                ", objectUrl='" + objectUrl + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", token='" + token + '\'' +
                '}';
    }

    public static CloudImportRequestBuilder builder() {
        return new CloudImportRequestBuilder();
    }

    public static class CloudImportRequestBuilder extends BaseImportRequestBuilder<CloudImportRequestBuilder> {
        private String clusterId;
        private String dbName;
        private String collectionName;
        private String partitionName;
        private List<List<String>> objectUrls;
        private String objectUrl;
        private String accessKey;
        private String secretKey;
        private String token;

        private CloudImportRequestBuilder() {
            this.clusterId = "";
            this.dbName = "";
            this.collectionName = "";
            this.partitionName = "";
            this.objectUrls = new ArrayList<>();
            this.objectUrl = "";
            this.accessKey = "";
            this.secretKey = "";
            this.token = "";
        }

        public CloudImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public CloudImportRequestBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public CloudImportRequestBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CloudImportRequestBuilder partitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public CloudImportRequestBuilder objectUrls(List<List<String>> objectUrls) {
            this.objectUrls = objectUrls;
            return this;
        }

        public CloudImportRequestBuilder objectUrl(String objectUrl) {
            this.objectUrl = objectUrl;
            return this;
        }

        public CloudImportRequestBuilder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public CloudImportRequestBuilder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public CloudImportRequestBuilder token(String token) {
            this.token = token;
            return this;
        }

        public CloudImportRequest build() {
            return new CloudImportRequest(this);
        }
    }
}