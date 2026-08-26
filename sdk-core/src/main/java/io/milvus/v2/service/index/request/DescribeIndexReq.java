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

/**
 * Request parameters for the {@code describeIndex} API.
 */
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

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     * @throws IllegalArgumentException if the collection name is null
     */
    public void setCollectionName(String collectionName) {
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    /**
     * Returns the field name.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the field name.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Sets the index name.
     *
     * @param indexName the index name
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Returns the timestamp used to filter the checked segments.
     *
     * @return the timestamp value; only segments generated before this timestamp are checked
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp used to filter the checked segments.
     *
     * @param timestamp the timestamp value; all segments are checked if this value is zero
     */
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

    /**
     * Creates a new builder for {@code DescribeIndexReq}.
     *
     * @return the builder
     */
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

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DescribeIndexReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         * @throws IllegalArgumentException if the collection name is null
         */
        public DescribeIndexReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the field name.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public DescribeIndexReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the index name.
         *
         * @param indexName the index name
         * @return this builder
         */
        public DescribeIndexReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the timestamp used to filter the checked segments.
         *
         * @param timestamp the timestamp value; all segments are checked if this value is zero
         * @return this builder
         */
        public DescribeIndexReqBuilder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Builds the {@code DescribeIndexReq}.
         *
         * @return the built request
         */
        public DescribeIndexReq build() {
            return new DescribeIndexReq(this);
        }
    }
}
