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
import io.milvus.param.bulkinsert.BulkInsertParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class BulkInsertParamTest {
    @Test
    void buildAndGet() {
        BulkInsertParam param = BulkInsertParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withPartitionName("p1")
                .withFiles(Arrays.asList("file1", "file2"))
                .withOption("key1", "value1")
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals("p1", param.getPartitionName());
        assertEquals(Arrays.asList("file1", "file2"), param.getFiles());
        assertEquals(Collections.singletonMap("key1", "value1"), param.getOptions());
    }

    @Test
    void addFile() {
        BulkInsertParam param = BulkInsertParam.newBuilder()
                .withCollectionName("coll1")
                .addFile("file1")
                .build();

        assertEquals(Collections.singletonList("file1"), param.getFiles());
    }

    @Test
    void defaults() {
        BulkInsertParam param = BulkInsertParam.newBuilder()
                .withCollectionName("coll1")
                .addFile("file1")
                .build();

        assertNull(param.getDatabaseName());
        assertNull(param.getPartitionName());
        assertEquals(0, param.getOptions().size());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> BulkInsertParam.newBuilder()
                .addFile("file1").build());
    }

    @Test
    void buildFailsWhenFilesEmpty() {
        assertThrows(ParamException.class, () -> BulkInsertParam.newBuilder()
                .withCollectionName("coll1").build());
    }

    @Test
    void buildFailsWhenFileEmpty() {
        assertThrows(ParamException.class, () -> BulkInsertParam.newBuilder()
                .withCollectionName("coll1")
                .addFile("")
                .build());
        assertThrows(ParamException.class, () -> BulkInsertParam.newBuilder()
                .withCollectionName("coll1")
                .withFiles(Collections.singletonList(null))
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                BulkInsertParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                BulkInsertParam.newBuilder().withFiles(null));
        assertThrows(IllegalArgumentException.class, () ->
                BulkInsertParam.newBuilder().addFile(null));
    }
}
