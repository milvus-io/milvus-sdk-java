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
import io.milvus.param.highlevel.dml.QuerySimpleParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class QuerySimpleParamTest {
    @Test
    void buildAndGet() {
        QuerySimpleParam param = QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withOutputFields(Arrays.asList("field1", "field2"))
                .withFilter("id > 0")
                .withOffset(5L)
                .withLimit(10L)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();

        assertEquals("coll1", param.getCollectionName());
        assertEquals(Arrays.asList("field1", "field2"), param.getOutputFields());
        assertEquals("id > 0", param.getFilter());
        assertEquals(Long.valueOf(5L), param.getOffset());
        assertEquals(Long.valueOf(10L), param.getLimit());
        assertEquals(ConsistencyLevelEnum.EVENTUALLY, param.getConsistencyLevel());
    }

    @Test
    void defaults() {
        QuerySimpleParam param = QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withFilter("id > 0")
                .build();

        assertEquals(0, param.getOutputFields().size());
        assertEquals(Long.valueOf(0L), param.getOffset());
        assertEquals(Long.valueOf(0L), param.getLimit());
        assertNull(param.getConsistencyLevel());
    }

    @Test
    void buildFailsWhenFilterBlank() {
        assertThrows(ParamException.class, () -> QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withFilter("")
                .build());
        assertThrows(ParamException.class, () -> QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .build());
    }

    @Test
    void buildFailsWhenOffsetNegative() {
        assertThrows(ParamException.class, () -> QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withFilter("id > 0")
                .withOffset(-1L)
                .build());
    }

    @Test
    void buildFailsWhenLimitNegative() {
        assertThrows(ParamException.class, () -> QuerySimpleParam.newBuilder()
                .withCollectionName("coll1")
                .withFilter("id > 0")
                .withLimit(-1L)
                .build());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> QuerySimpleParam.newBuilder()
                .withFilter("id > 0").build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                QuerySimpleParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                QuerySimpleParam.newBuilder().withOutputFields(null));
        assertThrows(IllegalArgumentException.class, () ->
                QuerySimpleParam.newBuilder().withFilter(null));
        assertThrows(IllegalArgumentException.class, () ->
                QuerySimpleParam.newBuilder().withOffset(null));
        assertThrows(IllegalArgumentException.class, () ->
                QuerySimpleParam.newBuilder().withLimit(null));
    }
}
