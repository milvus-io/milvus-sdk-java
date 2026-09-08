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

package io.milvus.unit.v1.param;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.PlaceholderType;
import io.milvus.param.MetricType;
import io.milvus.param.dml.SearchIteratorParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class SearchIteratorParamTest {

    private SearchIteratorParam.Builder baseBuilder() {
        return SearchIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)));
    }

    @Test
    void builderSetsAllFields() {
        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .withMetricType(MetricType.COSINE)
                .withVectorFieldName("vector")
                .withTopK(5)
                .withExpr("id > 1")
                .withOutFields(Arrays.asList("a", "b"))
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .withRoundDecimal(3)
                .withParams("{\"nprobe\":10}")
                .withIgnoreGrowing(true)
                .withGroupByFieldName("type")
                .withBatchSize(500L)
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals("COSINE", param.getMetricType());
        assertEquals("vector", param.getVectorFieldName());
        assertEquals(5L, param.getTopK());
        assertEquals("id > 1", param.getExpr());
        assertEquals(Arrays.asList("a", "b"), param.getOutFields());
        assertEquals(1L, param.getNQ());
        assertEquals(3, param.getRoundDecimal());
        assertEquals("{\"nprobe\":10}", param.getParams());
        assertTrue(param.isIgnoreGrowing());
        assertEquals("type", param.getGroupByFieldName());
        assertEquals(500L, param.getBatchSize());
        assertEquals(ConsistencyLevelEnum.BOUNDED, param.getConsistencyLevel());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
    }

    @Test
    void builderDefaults() {
        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getPartitionNames().isEmpty());
        assertEquals(-1L, param.getTopK());
        assertEquals("", param.getExpr());
        assertTrue(param.getOutFields().isEmpty());
        assertEquals(-1, param.getRoundDecimal());
        assertEquals("{}", param.getParams());
        assertEquals(0L, param.getTravelTimestamp());
        assertEquals(1L, param.getGuaranteeTimestamp());
        assertEquals(5000L, param.getGracefulTime());
        assertNull(param.getConsistencyLevel());
        assertFalse(param.isIgnoreGrowing());
        assertNull(param.getGroupByFieldName());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
        assertEquals(1000L, param.getBatchSize());
    }

    @Test
    void withLimitSetsTopK() {
        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(7L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();

        assertEquals(7L, param.getTopK());
    }

    @Test
    void withBinaryVectorsSetsPlaceholder() {
        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.HAMMING)
                .withBinaryVectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.BinaryVector, param.getPlType());
    }

    @Test
    void withSparseFloatVectorsSetsPlaceholder() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(1L, 0.1F);

        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.IP)
                .withSparseFloatVectors(Collections.singletonList(sparse))
                .build();

        assertEquals(PlaceholderType.SparseFloatVector, param.getPlType());
    }

    @Test
    void missingMetricTypeIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withLimit(10L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void zeroTopKIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withLimit(0L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchIteratorParam.newBuilder()
                        .withCollectionName("")
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void missingVectorsIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .build());
    }

    @Test
    void multipleFloatVectorsAreRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withFloatVectors(Arrays.asList(
                                Arrays.asList(0.1F, 0.2F),
                                Arrays.asList(0.3F, 0.4F)))
                        .build());
    }

    @Test
    void nullParamValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withPartitionNames(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withMetricType(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withVectorFieldName(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withTopK(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withLimit(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withExpr(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withOutFields(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().addOutField(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().addPartitionName(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withFloatVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withBinaryVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withFloat16Vectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withBFloat16Vectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withSparseFloatVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withRoundDecimal(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withParams(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withIgnoreGrowing(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withGroupByFieldName(null));
    }
}
