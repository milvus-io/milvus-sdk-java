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
import io.milvus.param.dml.SearchParam;

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
class SearchParamTest {

    private SearchParam.Builder baseBuilder() {
        return SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)));
    }

    @Test
    void builderSetsAllFields() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(1L, 0.1F);
        sparse.put(5L, 0.2F);

        SearchParam param = SearchParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .withMetricType(MetricType.COSINE)
                .withVectorFieldName("vector")
                .withTopK(5)
                .withExpr("id > 1")
                .withOutFields(Arrays.asList("a", "b"))
                .withFloatVectors(Arrays.asList(
                        Arrays.asList(0.1F, 0.2F, 0.3F, 0.4F),
                        Arrays.asList(0.5F, 0.6F, 0.7F, 0.8F)))
                .withRoundDecimal(3)
                .withParams("{\"nprobe\":10}")
                .withIgnoreGrowing(true)
                .withGroupByFieldName("type")
                .withGroupSize(2)
                .withStrictGroupSize(true)
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
        assertEquals(2L, param.getNQ());
        assertEquals(3, param.getRoundDecimal());
        assertEquals("{\"nprobe\":10}", param.getParams());
        assertTrue(param.isIgnoreGrowing());
        assertEquals("type", param.getGroupByFieldName());
        assertEquals(2, param.getGroupSize());
        assertEquals(Boolean.TRUE, param.getStrictGroupSize());
        assertEquals(ConsistencyLevelEnum.BOUNDED, param.getConsistencyLevel());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
    }

    @Test
    void builderDefaults() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getPartitionNames().isEmpty());
        assertEquals("None", param.getMetricType());
        assertEquals("", param.getExpr());
        assertEquals(-1, param.getRoundDecimal());
        assertEquals("{}", param.getParams());
        assertEquals(0L, param.getTravelTimestamp());
        assertEquals(1L, param.getGuaranteeTimestamp());
        assertEquals(5000L, param.getGracefulTime());
        assertNull(param.getConsistencyLevel());
        assertFalse(param.isIgnoreGrowing());
        assertNull(param.getGroupByFieldName());
        assertNull(param.getGroupSize());
        assertNull(param.getStrictGroupSize());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
    }

    @Test
    void withLimitSetsTopKAndNQ() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(7L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();

        assertEquals(7L, param.getTopK());
        assertEquals(1L, param.getNQ());
    }

    @Test
    void withBinaryVectorsSetsBinaryPlaceholder() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withBinaryVectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.BinaryVector, param.getPlType());
        assertEquals(1L, param.getNQ());
    }

    @Test
    void withFloat16VectorsSetsPlaceholder() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withFloat16Vectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.Float16Vector, param.getPlType());
    }

    @Test
    void withBFloat16VectorsSetsPlaceholder() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withBFloat16Vectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.BFloat16Vector, param.getPlType());
    }

    @Test
    void withSparseFloatVectorsSetsPlaceholder() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(1L, 0.1F);

        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withSparseFloatVectors(Collections.singletonList(sparse))
                .build();

        assertEquals(PlaceholderType.SparseFloatVector, param.getPlType());
        assertEquals(1L, param.getNQ());
    }

    @Test
    void setDatabaseNameUpdatesDatabaseName() {
        SearchParam param = baseBuilder().build();
        param.setDatabaseName("updated");

        assertEquals("updated", param.getDatabaseName());
    }

    @Test
    void addPartitionNameAndOutFieldAccumulate() {
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("coll")
                .withVectorFieldName("vector")
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .addPartitionName("p1")
                .addPartitionName("p1")
                .addPartitionName("p2")
                .addOutField("f1")
                .addOutField("f1")
                .addOutField("f2")
                .build();

        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals(Arrays.asList("f1", "f2"), param.getOutFields());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SearchParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("")
                        .withVectorFieldName("vector")
                        .withLimit(10L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void emptyVectorFieldNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("")
                        .withLimit(10L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void zeroTopKIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withLimit(0L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void missingVectorsIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withLimit(10L)
                        .build());
    }

    @Test
    void negativeGroupSizeIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withLimit(10L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .withGroupByFieldName("type")
                        .withGroupSize(0)
                        .build());
    }

    @Test
    void mismatchedVectorDimensionIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> SearchParam.newBuilder()
                        .withCollectionName("coll")
                        .withVectorFieldName("vector")
                        .withLimit(10L)
                        .withFloatVectors(Arrays.asList(
                                Arrays.asList(0.1F, 0.2F),
                                Arrays.asList(0.3F, 0.4F, 0.5F)))
                        .build());
    }

    @Test
    void verifyVectorsAcceptsValidFloatVectors() {
        SearchParam.verifyVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)));
    }

    @Test
    void verifyVectorsRejectsNull() {
        assertThrows(ParamException.class, () -> SearchParam.verifyVectors(null));
    }

    @Test
    void verifyVectorsRejectsEmpty() {
        assertThrows(ParamException.class, () -> SearchParam.verifyVectors(Collections.emptyList()));
    }

    @Test
    void verifyVectorsRejectsNonFloatList() {
        assertThrows(ParamException.class,
                () -> SearchParam.verifyVectors(Collections.singletonList(Arrays.asList("a", "b"))));
    }

    @Test
    void verifyVectorsRejectsIllegalType() {
        assertThrows(ParamException.class,
                () -> SearchParam.verifyVectors(Collections.singletonList(new Object())));
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
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withGroupSize(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withStrictGroupSize(null));
    }
}
