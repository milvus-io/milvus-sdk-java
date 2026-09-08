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

package io.milvus.unit.v2.service.vector.request.aggregation;

import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class SearchAggregationTest {

    @Test
    void builderSetsAllFields() {
        MetricSpec metric = MetricSpec.builder().op(MetricOps.AVG).fieldName("score").build();
        Map<String, MetricSpec> metrics = new LinkedHashMap<>();
        metrics.put("avg_score", metric);
        OrderSpec order = OrderSpec.builder().key("avg_score").direction(AggDirection.DESC).build();
        TopHitsSpec topHits = TopHitsSpec.builder().size(3).build();
        SearchAggregation sub = SearchAggregation.builder().fields(Collections.singletonList("tag")).size(5).build();

        SearchAggregation agg = SearchAggregation.builder()
                .fields(Arrays.asList("color", "size"))
                .size(10)
                .metrics(metrics)
                .order(Collections.singletonList(order))
                .topHits(topHits)
                .subAggregation(sub)
                .build();

        assertEquals(Arrays.asList("color", "size"), agg.getFields());
        assertEquals(10, agg.getSize());
        assertEquals(metric, agg.getMetrics().get("avg_score"));
        assertEquals(Collections.singletonList(order), agg.getOrder());
        assertEquals(topHits, agg.getTopHits());
        assertEquals(sub, agg.getSubAggregation());
    }

    @Test
    void builderAddMethodsAccumulate() {
        SearchAggregation agg = SearchAggregation.builder()
                .addField("color")
                .addField("size")
                .size(5)
                .addMetric("m1", MetricSpec.builder().op(MetricOps.COUNT).fieldName("*").build())
                .addOrder(OrderSpec.builder().key("_count").direction(AggDirection.DESC).build())
                .build();

        assertEquals(Arrays.asList("color", "size"), agg.getFields());
        assertEquals(1, agg.getMetrics().size());
        assertEquals(1, agg.getOrder().size());
        assertNull(agg.getTopHits());
        assertNull(agg.getSubAggregation());
    }

    @Test
    void builderReplaceMethodsClearPrevious() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Arrays.asList("a", "b"))
                .fields(Collections.singletonList("c"))
                .size(5)
                .build();
        assertEquals(Collections.singletonList("c"), agg.getFields());
    }

    @Test
    void validationRejectsEmptyFields() {
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder().size(5).build());
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder().fields(Collections.singletonList("")).size(5).build());
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder().fields(Arrays.asList("a", null)).size(5).build());
    }

    @Test
    void validationRejectsNonPositiveSize() {
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder().fields(Collections.singletonList("a")).size(0).build());
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder().fields(Collections.singletonList("a")).size(-1).build());
    }

    @Test
    void validationRejectsUnknownOrderKey() {
        assertThrows(MilvusClientException.class,
                () -> SearchAggregation.builder()
                        .fields(Collections.singletonList("a"))
                        .size(5)
                        .addOrder(OrderSpec.builder().key("unknown").direction(AggDirection.ASC).build())
                        .build());
    }

    @Test
    void specialOrderKeysAccepted() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("a"))
                .size(5)
                .addOrder(OrderSpec.builder().key("_count").direction(AggDirection.DESC).build())
                .addOrder(OrderSpec.builder().key("_key").direction(AggDirection.ASC).build())
                .build();
        assertEquals(2, agg.getOrder().size());
    }

    @Test
    void metricAliasOrderKeyAccepted() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("a"))
                .size(5)
                .addMetric("total", MetricSpec.builder().op(MetricOps.SUM).fieldName("price").build())
                .addOrder(OrderSpec.builder().key("total").direction(AggDirection.DESC).build())
                .build();
        assertEquals(1, agg.getOrder().size());
    }

    @Test
    void addMetricRejectsEmptyAliasAndNullMetric() {
        SearchAggregation.SearchAggregationBuilder b = SearchAggregation.builder().size(5);
        assertThrows(MilvusClientException.class,
                () -> b.addMetric("", MetricSpec.builder().op(MetricOps.COUNT).fieldName("*").build()));
        assertThrows(MilvusClientException.class,
                () -> b.addMetric(null, MetricSpec.builder().op(MetricOps.COUNT).fieldName("*").build()));
        assertThrows(MilvusClientException.class,
                () -> b.addMetric("m", null));
    }

    @Test
    void addOrderRejectsNull() {
        SearchAggregation.SearchAggregationBuilder b = SearchAggregation.builder().size(5);
        assertThrows(MilvusClientException.class, () -> b.addOrder(null));
    }

    @Test
    void collectionsAreUnmodifiable() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("a"))
                .size(5)
                .build();
        assertThrows(UnsupportedOperationException.class, () -> agg.getFields().add("b"));
        assertThrows(UnsupportedOperationException.class, () -> agg.getMetrics().put("x", null));
        assertThrows(UnsupportedOperationException.class, () -> agg.getOrder().add(null));
    }

    @Test
    void toProtoMapsNestedSpecs() {
        MetricSpec metric = MetricSpec.builder().op(MetricOps.AVG).fieldName("score").build();
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("color"))
                .size(3)
                .addMetric("avg_score", metric)
                .addOrder(OrderSpec.builder().key("_count").direction(AggDirection.DESC).nullFirst(true).build())
                .topHits(TopHitsSpec.builder().size(2).addSort(
                        SortSpec.builder().fieldName("pk").direction(AggDirection.ASC).build()).build())
                .build();

        io.milvus.grpc.SearchAggregationSpec grpc = agg.toProto();
        assertEquals(1, grpc.getFieldsCount());
        assertEquals("color", grpc.getFields(0));
        assertEquals(3, grpc.getSize());
        assertTrue(grpc.getMetricsMap().containsKey("avg_score"));
        assertEquals("avg", grpc.getMetricsMap().get("avg_score").getOp());
        assertEquals("score", grpc.getMetricsMap().get("avg_score").getFieldName());
        assertEquals(1, grpc.getOrderCount());
        assertEquals("_count", grpc.getOrder(0).getKey());
        assertTrue(grpc.hasTopHits());
        assertEquals(2, grpc.getTopHits().getSize());
    }

    @Test
    void toStringContainsFields() {
        SearchAggregation agg = SearchAggregation.builder()
                .fields(Collections.singletonList("a")).size(5).build();
        assertNotNull(agg.toString());
    }
}
