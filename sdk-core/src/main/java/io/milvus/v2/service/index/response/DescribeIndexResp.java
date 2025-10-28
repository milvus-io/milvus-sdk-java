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

public class DescribeIndexResp {
    private List<IndexDesc> indexDescriptions;

    private DescribeIndexResp(DescribeIndexRespBuilder builder) {
        this.indexDescriptions = builder.indexDescriptions;
    }

    public List<IndexDesc> getIndexDescriptions() {
        return indexDescriptions;
    }

    public void setIndexDescriptions(List<IndexDesc> indexDescriptions) {
        this.indexDescriptions = indexDescriptions;
    }

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

    public static DescribeIndexRespBuilder builder() {
        return new DescribeIndexRespBuilder();
    }

    public static class DescribeIndexRespBuilder {
        private List<IndexDesc> indexDescriptions = new ArrayList<>();

        private DescribeIndexRespBuilder() {
        }

        public DescribeIndexRespBuilder indexDescriptions(List<IndexDesc> indexDescriptions) {
            this.indexDescriptions = indexDescriptions;
            return this;
        }

        public DescribeIndexResp build() {
            return new DescribeIndexResp(this);
        }
    }

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

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public IndexParam.IndexType getIndexType() {
            return indexType;
        }

        public void setIndexType(IndexParam.IndexType indexType) {
            this.indexType = indexType;
        }

        public IndexParam.MetricType getMetricType() {
            return metricType;
        }

        public void setMetricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
        }

        public Map<String, String> getExtraParams() {
            return extraParams;
        }

        public void setExtraParams(Map<String, String> extraParams) {
            this.extraParams = extraParams;
        }

        public long getIndexedRows() {
            return indexedRows;
        }

        public void setIndexedRows(long indexedRows) {
            this.indexedRows = indexedRows;
        }

        public long getTotalRows() {
            return totalRows;
        }

        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
        }

        public long getPendingIndexRows() {
            return pendingIndexRows;
        }

        public void setPendingIndexRows(long pendingIndexRows) {
            this.pendingIndexRows = pendingIndexRows;
        }

        public IndexBuildState getIndexState() {
            return indexState;
        }

        public void setIndexState(IndexBuildState indexState) {
            this.indexState = indexState;
        }

        public String getIndexFailedReason() {
            return indexFailedReason;
        }

        public void setIndexFailedReason(String indexFailedReason) {
            this.indexFailedReason = indexFailedReason;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

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

            public IndexDescBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            public IndexDescBuilder indexName(String indexName) {
                this.indexName = indexName;
                return this;
            }

            public IndexDescBuilder id(long id) {
                this.id = id;
                return this;
            }

            public IndexDescBuilder indexType(IndexParam.IndexType indexType) {
                this.indexType = indexType;
                return this;
            }

            public IndexDescBuilder metricType(IndexParam.MetricType metricType) {
                this.metricType = metricType;
                return this;
            }

            public IndexDescBuilder extraParams(Map<String, String> extraParams) {
                this.extraParams = extraParams;
                return this;
            }

            public IndexDescBuilder indexedRows(long indexedRows) {
                this.indexedRows = indexedRows;
                return this;
            }

            public IndexDescBuilder totalRows(long totalRows) {
                this.totalRows = totalRows;
                return this;
            }

            public IndexDescBuilder pendingIndexRows(long pendingIndexRows) {
                this.pendingIndexRows = pendingIndexRows;
                return this;
            }

            public IndexDescBuilder indexState(IndexBuildState indexState) {
                this.indexState = indexState;
                return this;
            }

            public IndexDescBuilder indexFailedReason(String indexFailedReason) {
                this.indexFailedReason = indexFailedReason;
                return this;
            }

            public IndexDescBuilder properties(Map<String, String> properties) {
                this.properties = properties;
                return this;
            }

            public IndexDesc build() {
                return new IndexDesc(this);
            }
        }
    }
}
