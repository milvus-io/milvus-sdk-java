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

package io.milvus.unit.v2.service.vector.request;

import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AnnSearchReqTest {

    @Test
    void builderSetsAllFields() {
        FloatVec vec = new FloatVec(Arrays.asList(1.0f, 2.0f));
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        AnnSearchReq req = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .limit(10)
                .filter("pk > 3")
                .vectors(Collections.singletonList(vec))
                .params("{\"nprobe\": 10}")
                .metricType(IndexParam.MetricType.COSINE)
                .timezone("UTC")
                .filterTemplateValues(templateValues)
                .build();

        assertEquals("vector", req.getVectorFieldName());
        assertEquals(10, req.getLimit());
        assertEquals(10, req.getTopK());
        assertEquals("pk > 3", req.getFilter());
        assertEquals("pk > 3", req.getExpr());
        assertEquals(Collections.singletonList(vec), req.getVectors());
        assertEquals("{\"nprobe\": 10}", req.getParams());
        assertEquals(IndexParam.MetricType.COSINE, req.getMetricType());
        assertEquals("UTC", req.getTimezone());
        assertEquals(templateValues, req.getFilterTemplateValues());
    }

    @Test
    void builderDefaults() {
        AnnSearchReq req = AnnSearchReq.builder().build();

        assertNull(req.getVectorFieldName());
        assertEquals(0, req.getTopK());
        assertEquals(0L, req.getLimit());
        assertEquals("", req.getExpr());
        assertEquals("", req.getFilter());
        assertNull(req.getVectors());
        assertNull(req.getParams());
        assertNull(req.getMetricType());
        assertEquals("", req.getTimezone());
        assertNotNull(req.getFilterTemplateValues());
        assertTrue(req.getFilterTemplateValues().isEmpty());
    }

    @Test
    void topKAndLimitRemainInSync() {
        AnnSearchReq req = AnnSearchReq.builder().topK(4).build();
        assertEquals(4, req.getTopK());
        assertEquals(4L, req.getLimit());

        req.setTopK(6);
        assertEquals(6, req.getTopK());
        assertEquals(6L, req.getLimit());

        req.setLimit(8);
        assertEquals(8, req.getLimit());
        assertEquals(8, req.getTopK());
    }

    @Test
    void exprAndFilterRemainInSync() {
        AnnSearchReq req = AnnSearchReq.builder().expr("a == 1").build();
        assertEquals("a == 1", req.getExpr());
        assertEquals("a == 1", req.getFilter());

        req.setExpr("b == 2");
        assertEquals("b == 2", req.getExpr());
        assertEquals("b == 2", req.getFilter());

        req.setFilter("c == 3");
        assertEquals("c == 3", req.getFilter());
        assertEquals("c == 3", req.getExpr());
    }

    @Test
    void settersUpdateFields() {
        AnnSearchReq req = AnnSearchReq.builder().build();

        req.setVectorFieldName("vector2");
        req.setVectors(Collections.singletonList(new FloatVec(new float[]{1f})));
        req.setParams("{}");
        req.setMetricType(IndexParam.MetricType.IP);

        assertEquals("vector2", req.getVectorFieldName());
        assertEquals(1, req.getVectors().size());
        assertEquals("{}", req.getParams());
        assertEquals(IndexParam.MetricType.IP, req.getMetricType());
    }

    @Test
    void toStringContainsFields() {
        AnnSearchReq req = AnnSearchReq.builder().vectorFieldName("vector").build();
        assertNotNull(req.toString());
    }
}
