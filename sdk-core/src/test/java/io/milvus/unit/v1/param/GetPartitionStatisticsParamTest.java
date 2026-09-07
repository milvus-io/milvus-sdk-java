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
import io.milvus.param.partition.GetPartitionStatisticsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetPartitionStatisticsParamTest {
    @Test
    void buildAndGet() {
        GetPartitionStatisticsParam param = GetPartitionStatisticsParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withPartitionName("p1")
                .withFlush(Boolean.TRUE)
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals("p1", param.getPartitionName());
        assertTrue(param.isFlushCollection());
    }

    @Test
    void defaults() {
        GetPartitionStatisticsParam param = GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("coll1")
                .withPartitionName("p1")
                .build();

        assertNull(param.getDatabaseName());
        assertFalse(param.isFlushCollection());
    }

    @Test
    void buildFailsWhenRequiredFieldMissing() {
        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withPartitionName("p1").build());
        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("coll1").build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                GetPartitionStatisticsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetPartitionStatisticsParam.newBuilder().withPartitionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetPartitionStatisticsParam.newBuilder().withFlush(null));
    }
}
