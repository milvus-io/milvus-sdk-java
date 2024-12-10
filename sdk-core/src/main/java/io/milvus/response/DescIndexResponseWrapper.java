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

package io.milvus.response;

import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.DescribeIndexResponse;

import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.IndexBuildState;
import io.milvus.param.MetricType;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.*;

/**
 * Util class to wrap response of <code>describeIndex</code> interface.
 */
public class DescIndexResponseWrapper {
    private final DescribeIndexResponse response;

    public DescIndexResponseWrapper(@NonNull DescribeIndexResponse response) {
        this.response = response;
    }

    /**
     * Get index description of fields.
     *
     * @return List of IndexDesc, index description of fields
     */
    public List<IndexDesc> getIndexDescriptions() {
        List<IndexDesc> results = new ArrayList<>();
        List<IndexDescription> descriptions = response.getIndexDescriptionsList();
        descriptions.forEach((desc)->{
            IndexDesc res = convertIndexDescInternal(desc);
            results.add(res);
        });

        return results;
    }

    /**
     * Get index description by field name.
     * Return null if the field doesn't exist
     *
     * @param fieldName field name to get index description
     * @return {@link IndexDesc} description of the index
     */
    public IndexDesc getIndexDescByFieldName(@NonNull String fieldName) {
        for (IndexDescription desc : response.getIndexDescriptionsList()) {
            if (fieldName.compareTo(desc.getFieldName()) == 0) {
                return convertIndexDescInternal(desc);
            }
        }

        return null;
    }

    /**
     * Get index description by index name.
     * Return null if the index doesn't exist
     *
     * @param indexName index name to get index description
     * @return {@link IndexDesc} description of the index
     */
    public IndexDesc getIndexDescByIndexName(@NonNull String indexName) {
        for (IndexDescription desc : response.getIndexDescriptionsList()) {
            if (indexName.compareTo(desc.getIndexName()) == 0) {
                return convertIndexDescInternal(desc);
            }
        }

        return null;
    }

    private IndexDesc convertIndexDescInternal(IndexDescription desc) {
        IndexDesc res = new IndexDesc(desc.getFieldName(), desc.getIndexName(), desc.getIndexID());
        res.indexedRows = desc.getIndexedRows();
        res.totalRows = desc.getTotalRows();
        res.pendingIndexRows = desc.getPendingIndexRows();
        res.indexState = IndexBuildState.valueOf(desc.getState().name());
        res.indexFailedReason = desc.getIndexStateFailReason();
        desc.getParamsList().forEach((kv)-> res.addParam(kv.getKey(), kv.getValue()));
        return res;
    }

    /**
     * Internal-use class to wrap response of <code>describeIndex</code> interface.
     */
    @Getter
    @ToString
    public static final class IndexDesc {
        private final String fieldName;
        private final String indexName;
        private final long id;
        private final Map<String, String> params = new HashMap<>();

        long indexedRows = 0;
        long totalRows = 0;
        long pendingIndexRows = 0;
        private IndexBuildState indexState = IndexBuildState.IndexStateNone;
        String indexFailedReason = "";

        public IndexDesc(@NonNull String fieldName, @NonNull String indexName, long id) {
            this.fieldName = fieldName;
            this.indexName = indexName;
            this.id = id;
        }

        public void addParam(@NonNull String key, @NonNull String value) {
            this.params.put(key, value);
        }

        public IndexType getIndexType() {
            if (this.params.containsKey(Constant.INDEX_TYPE)) {
                // may throw IllegalArgumentException
                return IndexType.valueOf(params.get(Constant.INDEX_TYPE).toUpperCase());
            }

            return IndexType.None;
        }

        public MetricType getMetricType() {
            if (this.params.containsKey(Constant.METRIC_TYPE)) {
                // may throw IllegalArgumentException
                return MetricType.valueOf(params.get(Constant.METRIC_TYPE));
            }

            return MetricType.None;
        }

        public String getExtraParam() {
            if (this.params.containsKey(Constant.PARAMS)) {
                // may throw IllegalArgumentException
                return params.get(Constant.PARAMS);
            }

            return "";
        }
    }
}
