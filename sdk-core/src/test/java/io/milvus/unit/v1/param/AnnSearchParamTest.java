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

import io.milvus.exception.ParamException;
import io.milvus.grpc.PlaceholderType;
import io.milvus.param.MetricType;
import io.milvus.param.dml.AnnSearchParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class AnnSearchParamTest {

    private AnnSearchParam.Builder baseBuilder() {
        return AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)));
    }

    @Test
    void builderSetsAllFields() {
        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withMetricType(MetricType.COSINE)
                .withVectorFieldName("vector")
                .withTopK(5)
                .withExpr("id > 1")
                .withFloatVectors(Arrays.asList(
                        Arrays.asList(0.1F, 0.2F),
                        Arrays.asList(0.3F, 0.4F)))
                .withParams("{\"nprobe\":10}")
                .build();

        assertEquals("COSINE", param.getMetricType());
        assertEquals("vector", param.getVectorFieldName());
        assertEquals(5L, param.getTopK());
        assertEquals("id > 1", param.getExpr());
        assertEquals(2L, param.getNQ());
        assertEquals("{\"nprobe\":10}", param.getParams());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
    }

    @Test
    void builderDefaults() {
        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                .build();

        assertEquals("L2", param.getMetricType());
        assertEquals(10L, param.getTopK());
        assertEquals("", param.getExpr());
        assertEquals("{}", param.getParams());
        assertEquals(PlaceholderType.FloatVector, param.getPlType());
    }

    @Test
    void withBinaryVectorsSetsPlaceholder() {
        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.HAMMING)
                .withLimit(10L)
                .withBinaryVectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.BinaryVector, param.getPlType());
        assertEquals(1L, param.getNQ());
    }

    @Test
    void withFloat16VectorsSetsPlaceholder() {
        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloat16Vectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.Float16Vector, param.getPlType());
    }

    @Test
    void withBFloat16VectorsSetsPlaceholder() {
        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withBFloat16Vectors(Collections.singletonList(ByteBuffer.wrap(new byte[]{1, 2})))
                .build();

        assertEquals(PlaceholderType.BFloat16Vector, param.getPlType());
    }

    @Test
    void withSparseFloatVectorsSetsPlaceholder() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(1L, 0.1F);

        AnnSearchParam param = AnnSearchParam.newBuilder()
                .withVectorFieldName("vector")
                .withMetricType(MetricType.IP)
                .withLimit(10L)
                .withSparseFloatVectors(Collections.singletonList(sparse))
                .build();

        assertEquals(PlaceholderType.SparseFloatVector, param.getPlType());
    }

    @Test
    void emptyVectorFieldNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AnnSearchParam.newBuilder()
                        .withVectorFieldName("")
                        .withMetricType(MetricType.L2)
                        .withLimit(10L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void zeroTopKIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AnnSearchParam.newBuilder()
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withLimit(0L)
                        .withFloatVectors(Collections.singletonList(Arrays.asList(0.1F, 0.2F)))
                        .build());
    }

    @Test
    void emptyVectorsIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AnnSearchParam.newBuilder()
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withLimit(10L)
                        .withFloatVectors(Collections.emptyList())
                        .build());
    }

    @Test
    void missingVectorsThrowsNullPointer() {
        assertThrows(NullPointerException.class,
                () -> AnnSearchParam.newBuilder()
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withLimit(10L)
                        .build());
    }

    @Test
    void mismatchedVectorDimensionIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AnnSearchParam.newBuilder()
                        .withVectorFieldName("vector")
                        .withMetricType(MetricType.L2)
                        .withLimit(10L)
                        .withFloatVectors(Arrays.asList(
                                Arrays.asList(0.1F, 0.2F),
                                Arrays.asList(0.3F, 0.4F, 0.5F)))
                        .build());
    }

    @Test
    void nullParamValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withMetricType(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withVectorFieldName(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withTopK(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withLimit(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withExpr(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withFloatVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withBinaryVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withFloat16Vectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withBFloat16Vectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withSparseFloatVectors(null));
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().withParams(null));
    }
}
