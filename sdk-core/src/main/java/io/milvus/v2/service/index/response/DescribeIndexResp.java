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

package io.milvus.v2.service.index.response;

import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response returned by the {@code describeIndex} API.
 */
public class DescribeIndexResp {
    private List<IndexDesc> indexDescriptions;

    private DescribeIndexResp(DescribeIndexRespBuilder builder) {
        this.indexDescriptions = builder.indexDescriptions;
    }

    /**
     * Returns the index descriptions of the collection.
     *
     * @return the list of index descriptions
     */
    public List<IndexDesc> getIndexDescriptions() {
        return indexDescriptions;
    }

    /**
     * Sets the index descriptions of the collection.
     *
     * @param indexDescriptions the list of index descriptions
     */
    public void setIndexDescriptions(List<IndexDesc> indexDescriptions) {
        this.indexDescriptions = indexDescriptions;
    }

    /**
     * Returns the index description of the specified field.
     *
     * @param fieldName the field name
     * @return the index description of the field, or {@code null} if not found
     * @throws IllegalArgumentException if the field name is null
     */
    public IndexDesc getIndexDescByFieldName(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name cannot be null");
        }
        for (IndexDesc desc : indexDescriptions) {
            if (desc.getFieldName().equals(fieldName)) {
                return desc;
            }
        }
        return null;
    }

    /**
     * Returns the index description of the specified index.
     *
     * @param indexName the index name
     * @return the index description of the index, or {@code null} if not found
     * @throws IllegalArgumentException if the index name is null
     */
    public IndexDesc getIndexDescByIndexName(String indexName) {
        if (indexName == null) {
            throw new IllegalArgumentException("Index name cannot be null");
        }
        for (IndexDesc desc : indexDescriptions) {
            if (desc.getIndexName().equals(indexName)) {
                return desc;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "DescribeIndexResp{" +
                "indexDescriptions=" + indexDescriptions +
                '}';
    }

    /**
     * Creates a new builder for {@code DescribeIndexResp}.
     *
     * @return the builder
     */
    public static DescribeIndexRespBuilder builder() {
        return new DescribeIndexRespBuilder();
    }

    public static class DescribeIndexRespBuilder {
        private List<IndexDesc> indexDescriptions = new ArrayList<>();

        private DescribeIndexRespBuilder() {
        }

        /**
         * Sets the index descriptions of the collection.
         *
         * @param indexDescriptions the list of index descriptions
         * @return this builder
         */
        public DescribeIndexRespBuilder indexDescriptions(List<IndexDesc> indexDescriptions) {
            this.indexDescriptions = indexDescriptions;
            return this;
        }

        /**
         * Builds the {@code DescribeIndexResp}.
         *
         * @return the built response
         */
        public DescribeIndexResp build() {
            return new DescribeIndexResp(this);
        }
    }

    /**
     * Describes a single index of a collection.
     */
    public static final class IndexDesc {
        private String fieldName;
        private String indexName;
        private long id;
        private IndexParam.IndexType indexType;
        private IndexParam.MetricType metricType;
        private Map<String, String> extraParams;
        private long indexedRows;
        private long totalRows;
        private long pendingIndexRows;
        private IndexBuildState indexState;
        private String indexFailedReason;

        // In 2.4/2.5, properties only contains one item "mmap.enabled".
        // To keep consistence with other SDKs, we intend to remove this member from IndexDesc,
        // and put "mmap.enabled" into the "extraParams", the reasons:
        //  (1) when createIndex() is call, "mmap.enabled" is passed by the IndexParam.extraParams
        //  (2) other SDKs don't have a "properties" member for describeIndex()
        //  (3) now the "mmap.enabled" is dispatched to "properties" by ConvertUtils.convertToDescribeIndexResp(),
        //      once there are new property available, the new property will be dispatched to "extraParams",
        //      the "properties" member is not maintainable.
        @Deprecated
        private Map<String, String> properties;

        private IndexDesc(IndexDescBuilder builder) {
            this.fieldName = builder.fieldName;
            this.indexName = builder.indexName;
            this.id = builder.id;
            this.indexType = builder.indexType;
            this.metricType = builder.metricType;
            this.extraParams = builder.extraParams;
            this.indexedRows = builder.indexedRows;
            this.totalRows = builder.totalRows;
            this.pendingIndexRows = builder.pendingIndexRows;
            this.indexState = builder.indexState;
            this.indexFailedReason = builder.indexFailedReason;
            this.properties = builder.properties;
        }

        /**
         * Returns the field name of the index.
         *
         * @return the field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Sets the field name of the index.
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
         * Returns the unique ID of the index.
         *
         * @return the index ID
         */
        public long getId() {
            return id;
        }

        /**
         * Sets the unique ID of the index.
         *
         * @param id the index ID
         */
        public void setId(long id) {
            this.id = id;
        }

        /**
         * Returns the index type.
         *
         * @return the index type
         */
        public IndexParam.IndexType getIndexType() {
            return indexType;
        }

        /**
         * Sets the index type.
         *
         * @param indexType the index type
         */
        public void setIndexType(IndexParam.IndexType indexType) {
            this.indexType = indexType;
        }

        /**
         * Returns the metric type used by the index.
         *
         * @return the metric type
         */
        public IndexParam.MetricType getMetricType() {
            return metricType;
        }

        /**
         * Sets the metric type used by the index.
         *
         * @param metricType the metric type
         */
        public void setMetricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
        }

        /**
         * Returns the extra parameters of the index.
         *
         * @return the extra parameters
         */
        public Map<String, String> getExtraParams() {
            return extraParams;
        }

        /**
         * Sets the extra parameters of the index.
         *
         * @param extraParams the extra parameters
         */
        public void setExtraParams(Map<String, String> extraParams) {
            this.extraParams = extraParams;
        }

        /**
         * Returns the number of rows already indexed.
         *
         * @return the number of indexed rows
         */
        public long getIndexedRows() {
            return indexedRows;
        }

        /**
         * Sets the number of rows already indexed.
         *
         * @param indexedRows the number of indexed rows
         */
        public void setIndexedRows(long indexedRows) {
            this.indexedRows = indexedRows;
        }

        /**
         * Returns the total number of rows in the index.
         *
         * @return the total number of rows
         */
        public long getTotalRows() {
            return totalRows;
        }

        /**
         * Sets the total number of rows in the index.
         *
         * @param totalRows the total number of rows
         */
        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
        }

        /**
         * Returns the number of rows waiting to be indexed.
         *
         * @return the number of pending index rows
         */
        public long getPendingIndexRows() {
            return pendingIndexRows;
        }

        /**
         * Sets the number of rows waiting to be indexed.
         *
         * @param pendingIndexRows the number of pending index rows
         */
        public void setPendingIndexRows(long pendingIndexRows) {
            this.pendingIndexRows = pendingIndexRows;
        }

        /**
         * Returns the state of the index build.
         *
         * @return the index build state
         */
        public IndexBuildState getIndexState() {
            return indexState;
        }

        /**
         * Sets the state of the index build.
         *
         * @param indexState the index build state
         */
        public void setIndexState(IndexBuildState indexState) {
            this.indexState = indexState;
        }

        /**
         * Returns the reason why the index build failed, if any.
         *
         * @return the index failed reason
         */
        public String getIndexFailedReason() {
            return indexFailedReason;
        }

        /**
         * Sets the reason why the index build failed, if any.
         *
         * @param indexFailedReason the index failed reason
         */
        public void setIndexFailedReason(String indexFailedReason) {
            this.indexFailedReason = indexFailedReason;
        }

        /**
         * Returns the properties of the index.
         *
         * @return the index properties
         */
        public Map<String, String> getProperties() {
            return properties;
        }

        /**
         * Sets the properties of the index.
         *
         * @param properties the index properties
         */
        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public String toString() {
            return "IndexDesc{" +
                    "fieldName='" + fieldName + '\'' +
                    ", indexName='" + indexName + '\'' +
                    ", id=" + id +
                    ", indexType=" + indexType +
                    ", metricType=" + metricType +
                    ", extraParams=" + extraParams +
                    ", indexedRows=" + indexedRows +
                    ", totalRows=" + totalRows +
                    ", pendingIndexRows=" + pendingIndexRows +
                    ", indexState=" + indexState +
                    ", indexFailedReason='" + indexFailedReason + '\'' +
                    ", properties=" + properties +
                    '}';
        }

        /**
         * Creates a new builder for {@code IndexDesc}.
         *
         * @return the builder
         */
        public static IndexDescBuilder builder() {
            return new IndexDescBuilder();
        }

        public static class IndexDescBuilder {
            private String fieldName;
            private String indexName;
            private long id;
            private IndexParam.IndexType indexType = IndexParam.IndexType.None;
            private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
            private Map<String, String> extraParams = new HashMap<>();
            private long indexedRows = 0;
            private long totalRows = 0;
            private long pendingIndexRows = 0;
            private IndexBuildState indexState = IndexBuildState.IndexStateNone;
            private String indexFailedReason = "";
            private Map<String, String> properties = new HashMap<>();

            private IndexDescBuilder() {
            }

            /**
             * Sets the field name of the index.
             *
             * @param fieldName the field name
             * @return this builder
             */
            public IndexDescBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            /**
             * Sets the index name.
             *
             * @param indexName the index name
             * @return this builder
             */
            public IndexDescBuilder indexName(String indexName) {
                this.indexName = indexName;
                return this;
            }

            /**
             * Sets the unique ID of the index.
             *
             * @param id the index ID
             * @return this builder
             */
            public IndexDescBuilder id(long id) {
                this.id = id;
                return this;
            }

            /**
             * Sets the index type.
             *
             * @param indexType the index type
             * @return this builder
             */
            public IndexDescBuilder indexType(IndexParam.IndexType indexType) {
                this.indexType = indexType;
                return this;
            }

            /**
             * Sets the metric type used by the index.
             *
             * @param metricType the metric type
             * @return this builder
             */
            public IndexDescBuilder metricType(IndexParam.MetricType metricType) {
                this.metricType = metricType;
                return this;
            }

            /**
             * Sets the extra parameters of the index.
             *
             * @param extraParams the extra parameters
             * @return this builder
             */
            public IndexDescBuilder extraParams(Map<String, String> extraParams) {
                this.extraParams = extraParams;
                return this;
            }

            /**
             * Sets the number of rows already indexed.
             *
             * @param indexedRows the number of indexed rows
             * @return this builder
             */
            public IndexDescBuilder indexedRows(long indexedRows) {
                this.indexedRows = indexedRows;
                return this;
            }

            /**
             * Sets the total number of rows in the index.
             *
             * @param totalRows the total number of rows
             * @return this builder
             */
            public IndexDescBuilder totalRows(long totalRows) {
                this.totalRows = totalRows;
                return this;
            }

            /**
             * Sets the number of rows waiting to be indexed.
             *
             * @param pendingIndexRows the number of pending index rows
             * @return this builder
             */
            public IndexDescBuilder pendingIndexRows(long pendingIndexRows) {
                this.pendingIndexRows = pendingIndexRows;
                return this;
            }

            /**
             * Sets the state of the index build.
             *
             * @param indexState the index build state
             * @return this builder
             */
            public IndexDescBuilder indexState(IndexBuildState indexState) {
                this.indexState = indexState;
                return this;
            }

            /**
             * Sets the reason why the index build failed, if any.
             *
             * @param indexFailedReason the index failed reason
             * @return this builder
             */
            public IndexDescBuilder indexFailedReason(String indexFailedReason) {
                this.indexFailedReason = indexFailedReason;
                return this;
            }

            /**
             * Sets the properties of the index.
             *
             * @param properties the index properties
             * @return this builder
             */
            public IndexDescBuilder properties(Map<String, String> properties) {
                this.properties = properties;
                return this;
            }

            /**
             * Builds the {@code IndexDesc}.
             *
             * @return the built index description
             */
            public IndexDesc build() {
                return new IndexDesc(this);
            }
        }
    }
}
