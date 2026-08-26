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

/**
 * Request parameters for the {@code describeCollection} API.
 */
public class DescribeCollectionReq {
    private String databaseName;
    private String collectionName;
    private Long collectionId;

    private DescribeCollectionReq(DescribeCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.collectionId = builder.collectionId;
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
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the collection ID.
     *
     * @return the collection ID
     */
    public Long getCollectionId() {
        return collectionId;
    }

    /**
     * Sets the collection ID.
     *
     * @param collectionId the collection ID
     */
    public void setCollectionId(Long collectionId) {
        this.collectionId = collectionId;
    }

    @Override
    public String toString() {
        return "DescribeCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", collectionId=" + collectionId +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeCollectionReq}.
     *
     * @return the builder
     */
    public static DescribeCollectionReqBuilder builder() {
        return new DescribeCollectionReqBuilder();
    }

    public static class DescribeCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Long collectionId;

        private DescribeCollectionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DescribeCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DescribeCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the collection ID.
         *
         * @param collectionId the collection ID
         * @return this builder
         */
        public DescribeCollectionReqBuilder collectionId(Long collectionId) {
            this.collectionId = collectionId;
            return this;
        }

        /**
         * Builds a {@link DescribeCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public DescribeCollectionReq build() {
            return new DescribeCollectionReq(this);
        }
    }
}
