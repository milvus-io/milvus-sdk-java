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

package io.milvus.bulkwriter;

import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.exception.ParamException;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class LocalBulkWriterParamTest {

    private static CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        return schema;
    }

    @Test
    void testBuildWithAllOptions() {
        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withLocalPath("/tmp/test_bulk_writer")
                .withChunkSize(1024L)
                .withFileType(BulkFileType.CSV)
                .withConfig("sep", "|")
                .build();

        assertNotNull(param.getCollectionSchema());
        assertEquals("/tmp/test_bulk_writer", param.getLocalPath());
        assertEquals(1024L, param.getChunkSize());
        assertEquals(BulkFileType.CSV, param.getFileType());
        assertEquals("|", param.getConfig().get("sep"));
        assertTrue(param.toString().contains("/tmp/test_bulk_writer"));
    }

    @Test
    void testBuildDefaults() {
        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withLocalPath("/tmp/test_bulk_writer")
                .build();

        assertEquals(128L * 1024 * 1024, param.getChunkSize());
        assertEquals(BulkFileType.PARQUET, param.getFileType());
    }

    @Test
    void testBuildRejectsEmptyLocalPath() {
        assertThrows(ParamException.class, () -> LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withLocalPath("")
                .build());
    }

    @Test
    void testBuildRejectsNullSchema() {
        assertThrows(ParamException.class, () -> LocalBulkWriterParam.newBuilder()
                .withLocalPath("/tmp/test_bulk_writer")
                .build());
    }
}
