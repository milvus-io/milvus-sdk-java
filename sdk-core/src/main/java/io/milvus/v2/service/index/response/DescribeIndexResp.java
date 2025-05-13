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
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class DescribeIndexResp {
    @Builder.Default
    private List<IndexDesc> indexDescriptions = new ArrayList<>();

    public IndexDesc getIndexDescByFieldName(@NonNull String fieldName) {
        for (IndexDesc desc : indexDescriptions) {
            if (desc.fieldName.equals(fieldName)) {
                return desc;
            }
        }
        return null;
    }

    public IndexDesc getIndexDescByIndexName(@NonNull String indexName) {
        for (IndexDesc desc : indexDescriptions) {
            if (desc.indexName.equals(indexName)) {
                return desc;
            }
        }
        return null;
    }

    @Data
    @SuperBuilder
    public static final class IndexDesc {
        private String fieldName;
        private String indexName;
        private long id;
        @Builder.Default
        private IndexParam.IndexType indexType = IndexParam.IndexType.None;
        @Builder.Default
        private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
        @Builder.Default
        private Map<String, String> extraParams = new HashMap<>();
        @Builder.Default
        private long indexedRows = 0;
        @Builder.Default
        private long totalRows = 0;
        @Builder.Default
        private long pendingIndexRows = 0;
        @Builder.Default
        private IndexBuildState indexState = IndexBuildState.IndexStateNone;
        @Builder.Default
        private String indexFailedReason = "";

        // In 2.4/2.5, properties only contains one item "mmap.enabled".
        // To keep consistence with other SDKs, we intend to remove this member from IndexDesc,
        // and put "mmap.enabled" into the "extraParams", the reasons:
        //  (1) when createIndex() is call, "mmap.enabled" is passed by the IndexParam.extraParams
        //  (2) other SDKs don't have a "properties" member for describeIndex()
        //  (3) now the "mmap.enabled" is dispatched to "properties" by ConvertUtils.convertToDescribeIndexResp(),
        //      once there are new property available, the new property will be dispatched to "extraParams",
        //      the "properties" member is not maintainable.
        @Builder.Default
        @Deprecated
        private Map<String, String> properties = new HashMap<>();
    }
}
