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

package io.milvus.unit.v2.service.index;

import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeIndexRespTest {

    private DescribeIndexResp.IndexDesc buildIndexDesc(String fieldName, String indexName) {
        return DescribeIndexResp.IndexDesc.builder()
                .fieldName(fieldName)
                .indexName(indexName)
                .build();
    }

    @Test
    void builderBuildsWithAllFields() {
        List<DescribeIndexResp.IndexDesc> descriptions = Arrays.asList(
                buildIndexDesc("vector", "idx_a"),
                buildIndexDesc("text", "idx_b"));
        DescribeIndexResp response = DescribeIndexResp.builder()
                .indexDescriptions(descriptions)
                .build();

        assertEquals(descriptions, response.getIndexDescriptions());
    }

    @Test
    void setterUpdatesGetter() {
        DescribeIndexResp response = DescribeIndexResp.builder().build();
        List<DescribeIndexResp.IndexDesc> descriptions = Arrays.asList(
                buildIndexDesc("vector", "idx_a"));
        response.setIndexDescriptions(descriptions);
        assertEquals(descriptions, response.getIndexDescriptions());
    }

    @Test
    void indexDescriptionsDefaultToEmptyList() {
        DescribeIndexResp response = DescribeIndexResp.builder().build();
        assertNotNull(response.getIndexDescriptions());
        assertTrue(response.getIndexDescriptions().isEmpty());
    }

    @Test
    void lookupByFieldNameAndIndexName() {
        DescribeIndexResp.IndexDesc vector = buildIndexDesc("vector", "idx_a");
        DescribeIndexResp.IndexDesc text = buildIndexDesc("text", "idx_b");
        DescribeIndexResp response = DescribeIndexResp.builder()
                .indexDescriptions(Arrays.asList(vector, text))
                .build();

        assertSame(vector, response.getIndexDescByFieldName("vector"));
        assertSame(text, response.getIndexDescByIndexName("idx_b"));
        assertNull(response.getIndexDescByFieldName("missing"));
        assertNull(response.getIndexDescByIndexName("missing"));
    }

    @Test
    void lookupsRejectNullArguments() {
        DescribeIndexResp response = DescribeIndexResp.builder()
                .indexDescriptions(Collections.singletonList(buildIndexDesc("vector", "idx_a")))
                .build();

        assertThrows(IllegalArgumentException.class, () -> response.getIndexDescByFieldName(null));
        assertThrows(IllegalArgumentException.class, () -> response.getIndexDescByIndexName(null));
    }

    @Test
    void indexDescBuilderBuildsWithAllFields() {
        Map<String, String> extraParams = Collections.singletonMap("mmap.enabled", "true");
        Map<String, String> properties = Collections.singletonMap("key", "value");
        DescribeIndexResp.IndexDesc desc = DescribeIndexResp.IndexDesc.builder()
                .fieldName("vector")
                .indexName("idx")
                .id(42L)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(extraParams)
                .indexedRows(100L)
                .totalRows(200L)
                .pendingIndexRows(300L)
                .indexState(IndexBuildState.Finished)
                .indexFailedReason("none")
                .properties(properties)
                .build();

        assertEquals("vector", desc.getFieldName());
        assertEquals("idx", desc.getIndexName());
        assertEquals(42L, desc.getId());
        assertEquals(IndexParam.IndexType.FLAT, desc.getIndexType());
        assertEquals(IndexParam.MetricType.IP, desc.getMetricType());
        assertEquals(extraParams, desc.getExtraParams());
        assertEquals(100L, desc.getIndexedRows());
        assertEquals(200L, desc.getTotalRows());
        assertEquals(300L, desc.getPendingIndexRows());
        assertEquals(IndexBuildState.Finished, desc.getIndexState());
        assertEquals("none", desc.getIndexFailedReason());
        assertEquals(properties, desc.getProperties());
    }

    @Test
    void indexDescDefaultsApply() {
        DescribeIndexResp.IndexDesc desc = DescribeIndexResp.IndexDesc.builder().build();

        assertNull(desc.getFieldName());
        assertNull(desc.getIndexName());
        assertEquals(0L, desc.getId());
        assertEquals(IndexParam.IndexType.None, desc.getIndexType());
        assertEquals(IndexParam.MetricType.INVALID, desc.getMetricType());
        assertTrue(desc.getExtraParams().isEmpty());
        assertEquals(0L, desc.getIndexedRows());
        assertEquals(0L, desc.getTotalRows());
        assertEquals(0L, desc.getPendingIndexRows());
        assertEquals(IndexBuildState.IndexStateNone, desc.getIndexState());
        assertEquals("", desc.getIndexFailedReason());
        assertTrue(desc.getProperties().isEmpty());
    }

    @Test
    void indexDescSettersUpdateGetters() {
        DescribeIndexResp.IndexDesc desc = DescribeIndexResp.IndexDesc.builder().build();

        desc.setFieldName("f2");
        assertEquals("f2", desc.getFieldName());

        desc.setIndexName("i2");
        assertEquals("i2", desc.getIndexName());

        desc.setId(7L);
        assertEquals(7L, desc.getId());

        desc.setIndexType(IndexParam.IndexType.HNSW);
        assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());

        desc.setMetricType(IndexParam.MetricType.COSINE);
        assertEquals(IndexParam.MetricType.COSINE, desc.getMetricType());

        Map<String, String> extra = Collections.singletonMap("k", "v");
        desc.setExtraParams(extra);
        assertEquals(extra, desc.getExtraParams());

        desc.setIndexedRows(1L);
        assertEquals(1L, desc.getIndexedRows());

        desc.setTotalRows(2L);
        assertEquals(2L, desc.getTotalRows());

        desc.setPendingIndexRows(3L);
        assertEquals(3L, desc.getPendingIndexRows());

        desc.setIndexState(IndexBuildState.Failed);
        assertEquals(IndexBuildState.Failed, desc.getIndexState());

        desc.setIndexFailedReason("oom");
        assertEquals("oom", desc.getIndexFailedReason());

        Map<String, String> props = Collections.singletonMap("p", "v");
        desc.setProperties(props);
        assertEquals(props, desc.getProperties());
    }

    @Test
    void indexDescBuilderFactory() {
        assertNotNull(DescribeIndexResp.IndexDesc.builder());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DescribeIndexResp.builder());
    }

    @Test
    void toStringContainsFields() {
        DescribeIndexResp response = DescribeIndexResp.builder()
                .indexDescriptions(Collections.singletonList(buildIndexDesc("vector", "idx")))
                .build();
        assertTrue(response.toString().contains("idx"));
    }
}
