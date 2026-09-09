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
import io.milvus.param.MetricType;
import io.milvus.param.QueryNodeSingleSearch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class QueryNodeSingleSearchTest {
    @Test
    void buildAndGetFloatVectors() {
        List<List<Float>> vectors = Arrays.asList(
                Arrays.asList(1.0f, 2.0f, 3.0f),
                Arrays.asList(4.0f, 5.0f, 6.0f));

        QueryNodeSingleSearch search = QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withMetricType(MetricType.IP)
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .withParams("{\"nprobe\":10}")
                .build();

        assertEquals("coll1", search.getCollectionName());
        assertEquals(MetricType.IP, search.getMetricType());
        assertEquals("vec", search.getVectorFieldName());
        assertEquals(vectors, search.getVectors());
        assertEquals("{\"nprobe\":10}", search.getParams());
    }

    @Test
    void defaults() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));

        QueryNodeSingleSearch search = QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build();

        assertEquals(MetricType.L2, search.getMetricType());
        assertEquals("{}", search.getParams());
    }

    @Test
    void buildWithBinaryVectors() {
        List<ByteBuffer> vectors = Arrays.asList(
                ByteBuffer.allocate(4), ByteBuffer.allocate(4));

        QueryNodeSingleSearch search = QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build();

        assertEquals(vectors, search.getVectors());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build());
    }

    @Test
    void buildFailsWhenVectorFieldNameMissing() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .build());
    }

    @Test
    void buildFailsWhenVectorsEmpty() {
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(new ArrayList<>())
                .build());
    }

    @Test
    void buildFailsWhenFloatVectorValueWrongType() {
        List<List<Integer>> vectors = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build());
    }

    @Test
    void buildFailsWhenFloatVectorDimensionsMismatch() {
        List<List<Float>> vectors = Arrays.asList(
                Arrays.asList(1.0f, 2.0f),
                Arrays.asList(3.0f, 4.0f, 5.0f));
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build());
    }

    @Test
    void buildFailsWhenBinaryVectorDimensionsMismatch() {
        List<ByteBuffer> vectors = Arrays.asList(
                ByteBuffer.allocate(4), ByteBuffer.allocate(8));
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build());
    }

    @Test
    void buildFailsWhenVectorTypeUnsupported() {
        List<String> vectors = Arrays.asList("not-a-vector");
        assertThrows(ParamException.class, () -> QueryNodeSingleSearch.newBuilder()
                .withCollectionName("coll1")
                .withVectorFieldName("vec")
                .withVectors(vectors)
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                QueryNodeSingleSearch.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                QueryNodeSingleSearch.newBuilder().withMetricType(null));
        assertThrows(IllegalArgumentException.class, () ->
                QueryNodeSingleSearch.newBuilder().withVectorFieldName(null));
        assertThrows(IllegalArgumentException.class, () ->
                QueryNodeSingleSearch.newBuilder().withVectors(null));
        assertThrows(IllegalArgumentException.class, () ->
                QueryNodeSingleSearch.newBuilder().withParams(null));
    }
}
