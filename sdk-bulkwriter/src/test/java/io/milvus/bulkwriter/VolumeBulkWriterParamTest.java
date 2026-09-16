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
import io.milvus.bulkwriter.common.clientenum.ConnectType;
import io.milvus.exception.ParamException;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class VolumeBulkWriterParamTest {

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
        VolumeBulkWriterParam param = VolumeBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/volume_writer")
                .withChunkSize(1024L)
                .withFileType(BulkFileType.JSON)
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("api-key")
                .withVolumeName("volume-1")
                .withConnectType(ConnectType.PUBLIC)
                .build();

        assertNotNull(param.getCollectionSchema());
        assertEquals("/tmp/volume_writer", param.getRemotePath());
        assertEquals(1024L, param.getChunkSize());
        assertEquals(BulkFileType.JSON, param.getFileType());
        assertEquals("https://api.cloud.zilliz.com", param.getCloudEndpoint());
        assertEquals("api-key", param.getApiKey());
        assertEquals("volume-1", param.getVolumeName());
        assertEquals(ConnectType.PUBLIC, param.getConnectType());
        assertTrue(param.toString().contains("volume-1"));
    }

    @Test
    void testBuildDefaults() {
        VolumeBulkWriterParam param = VolumeBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/volume_writer")
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("api-key")
                .withVolumeName("volume-1")
                .build();

        assertEquals(128L * 1024 * 1024, param.getChunkSize());
        assertEquals(BulkFileType.PARQUET, param.getFileType());
        assertEquals(ConnectType.AUTO, param.getConnectType());
    }

    @Test
    void testBuildRejectsMissingRequiredFields() {
        VolumeBulkWriterParam.Builder builder = VolumeBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/volume_writer");

        assertThrows(ParamException.class, () -> builder.build());
        assertThrows(ParamException.class, () -> builder.withCloudEndpoint("https://api.cloud.zilliz.com").build());
        assertThrows(ParamException.class, () -> builder.withApiKey("api-key").build());
    }
}
