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
import io.milvus.param.Constant;
import io.milvus.param.highlevel.dml.SearchSimpleParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SearchSimpleParamTest {
    @Test
    void buildAndGet() {
        List<List<Float>> vectors = Arrays.asList(
                Arrays.asList(1.0f, 2.0f),
                Arrays.asList(3.0f, 4.0f));

        SearchSimpleParam param = SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .withFilter("id > 0")
                .withOutputFields(Arrays.asList("field1"))
                .withOffset(5L)
                .withLimit(20L)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        assertEquals("coll1", param.getCollectionName());
        assertEquals(vectors, param.getVectors());
        assertEquals("id > 0", param.getFilter());
        assertEquals(Arrays.asList("field1"), param.getOutputFields());
        assertEquals(Long.valueOf(5L), param.getOffset());
        assertEquals(20, param.getLimit());
        assertEquals(ConsistencyLevelEnum.STRONG, param.getConsistencyLevel());
    }

    @Test
    void defaults() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));

        SearchSimpleParam param = SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .build();

        assertEquals("", param.getFilter());
        assertEquals(Long.valueOf(0L), param.getOffset());
        assertEquals(10, param.getLimit());
        assertNull(param.getConsistencyLevel());
        assertEquals(0, param.getOutputFields().size());
    }

    @Test
    void paramsContainOffsetAfterBuild() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));

        SearchSimpleParam param = SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .withOffset(3L)
                .build();

        assertEquals(3L, param.getParams().get(Constant.OFFSET));
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));
        assertThrows(ParamException.class, () -> SearchSimpleParam.newBuilder()
                .withVectors(vectors).build());
    }

    @Test
    void buildFailsWhenVectorsEmpty() {
        assertThrows(ParamException.class, () -> SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .build());
        assertThrows(ParamException.class, () -> SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(new ArrayList<>())
                .build());
    }

    @Test
    void buildFailsWhenOffsetNegative() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));
        assertThrows(ParamException.class, () -> SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .withOffset(-1L)
                .build());
    }

    @Test
    void buildFailsWhenLimitNegative() {
        List<List<Float>> vectors = new ArrayList<>();
        vectors.add(Arrays.asList(1.0f, 2.0f));
        assertThrows(ParamException.class, () -> SearchSimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withVectors(vectors)
                .withLimit(-1L)
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withVectors(null));
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withFilter(null));
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withOutputFields(null));
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withOffset(null));
        assertThrows(IllegalArgumentException.class, () ->
                SearchSimpleParam.newBuilder().withLimit(null));
    }
}
