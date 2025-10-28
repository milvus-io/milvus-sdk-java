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

package io.milvus.v2.service.index.request;

public class DescribeIndexReq {
    private String databaseName;
    private String collectionName;
    private String fieldName;
    private String indexName;
    private Long timestamp = 0L; // only check segments generated before this timestamp. all the segments will be checked if this value is zero.

    private DescribeIndexReq(DescribeIndexReqBuilder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.indexName = builder.indexName;
        this.timestamp = builder.timestamp;
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
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DescribeIndexReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static DescribeIndexReqBuilder builder() {
        return new DescribeIndexReqBuilder();
    }

    public static class DescribeIndexReqBuilder {
        private String databaseName;
        private String collectionName;
        private String fieldName;
        private String indexName;
        private Long timestamp = 0L; // only check segments generated before this timestamp. all the segments will be checked if this value is zero.

        private DescribeIndexReqBuilder() {
        }

        public DescribeIndexReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DescribeIndexReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        public DescribeIndexReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public DescribeIndexReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public DescribeIndexReqBuilder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public DescribeIndexReq build() {
            return new DescribeIndexReq(this);
        }
    }
}
