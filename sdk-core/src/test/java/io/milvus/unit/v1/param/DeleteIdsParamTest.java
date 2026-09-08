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
import io.milvus.param.highlevel.dml.DeleteIdsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class DeleteIdsParamTest {
    @Test
    void buildAndGet() {
        List<Long> ids = Arrays.asList(1L, 2L);

        DeleteIdsParam param = DeleteIdsParam.newBuilder()
                .withCollectionName("coll1")
                .withPartitionName("p1")
                .withPrimaryIds(ids)
                .build();

        assertEquals("coll1", param.getCollectionName());
        assertEquals("p1", param.getPartitionName());
        assertEquals(ids, param.getPrimaryIds());
    }

    @Test
    void partitionNameDefaultsToEmpty() {
        DeleteIdsParam param = DeleteIdsParam.newBuilder()
                .withCollectionName("coll1")
                .addPrimaryId(5L)
                .build();

        assertEquals("", param.getPartitionName());
    }

    @Test
    void addPrimaryId() {
        DeleteIdsParam param = DeleteIdsParam.newBuilder()
                .withCollectionName("coll1")
                .addPrimaryId(9L)
                .build();

        assertEquals(Arrays.asList(9L), param.getPrimaryIds());
    }

    @Test
    void buildFailsWhenPrimaryIdsEmpty() {
        assertThrows(ParamException.class, () -> DeleteIdsParam.newBuilder()
                .withCollectionName("coll1")
                .build());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> DeleteIdsParam.newBuilder()
                .addPrimaryId(9L).build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                DeleteIdsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                DeleteIdsParam.newBuilder().withPartitionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                DeleteIdsParam.newBuilder().withPrimaryIds(null));
        assertThrows(IllegalArgumentException.class, () ->
                DeleteIdsParam.newBuilder().addPrimaryId(null));
    }
}
