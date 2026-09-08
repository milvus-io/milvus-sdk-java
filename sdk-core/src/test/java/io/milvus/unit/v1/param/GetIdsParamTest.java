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
import io.milvus.param.highlevel.dml.GetIdsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class GetIdsParamTest {
    @Test
    void buildAndGet() {
        List<Long> ids = Arrays.asList(1L, 2L, 3L);

        GetIdsParam param = GetIdsParam.newBuilder()
                .withCollectionName("coll1")
                .withPrimaryIds(ids)
                .withOutputFields(Arrays.asList("field1"))
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .build();

        assertEquals("coll1", param.getCollectionName());
        assertEquals(ids, param.getPrimaryIds());
        assertEquals(Arrays.asList("field1"), param.getOutputFields());
        assertEquals(ConsistencyLevelEnum.BOUNDED, param.getConsistencyLevel());
    }

    @Test
    void addPrimaryId() {
        GetIdsParam param = GetIdsParam.newBuilder()
                .withCollectionName("coll1")
                .addPrimaryId(7L)
                .build();

        assertEquals(Arrays.asList(7L), param.getPrimaryIds());
    }

    @Test
    void defaults() {
        GetIdsParam param = GetIdsParam.newBuilder()
                .withCollectionName("coll1")
                .addPrimaryId(7L)
                .build();

        assertEquals(0, param.getOutputFields().size());
        assertNull(param.getConsistencyLevel());
    }

    @Test
    void buildFailsWhenPrimaryIdsEmpty() {
        assertThrows(ParamException.class, () -> GetIdsParam.newBuilder()
                .withCollectionName("coll1")
                .build());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> GetIdsParam.newBuilder()
                .addPrimaryId(7L).build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                GetIdsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetIdsParam.newBuilder().withPrimaryIds(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetIdsParam.newBuilder().addPrimaryId(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetIdsParam.newBuilder().withOutputFields(null));
    }
}
