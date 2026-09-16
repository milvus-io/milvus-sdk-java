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
import io.milvus.bulkwriter.connect.S3ConnectParam;
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
public class RemoteBulkWriterParamTest {

    private static CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        return schema;
    }

    private static S3ConnectParam buildConnectParam() {
        return S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint("http://127.0.0.1:9000")
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .build();
    }

    @Test
    void testBuildWithAllOptions() {
        RemoteBulkWriterParam param = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withConnectParam(buildConnectParam())
                .withRemotePath("/tmp/remote_writer")
                .withChunkSize(1024L)
                .withFileType(BulkFileType.JSON)
                .withConfig("sep", ",")
                .build();

        assertNotNull(param.getCollectionSchema());
        assertNotNull(param.getConnectParam());
        assertEquals("/tmp/remote_writer", param.getRemotePath());
        assertEquals(1024L, param.getChunkSize());
        assertEquals(BulkFileType.JSON, param.getFileType());
        assertEquals(",", param.getConfig().get("sep"));
        assertTrue(param.toString().contains("/tmp/remote_writer"));
    }

    @Test
    void testBuildDefaults() {
        RemoteBulkWriterParam param = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withConnectParam(buildConnectParam())
                .withRemotePath("/tmp/remote_writer")
                .build();

        assertEquals(128L * 1024 * 1024, param.getChunkSize());
        assertEquals(BulkFileType.PARQUET, param.getFileType());
    }

    @Test
    void testBuildRejectsEmptyRemotePath() {
        assertThrows(ParamException.class, () -> RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withConnectParam(buildConnectParam())
                .withRemotePath("")
                .build());
    }

    @Test
    void testBuildRejectsNullSchema() {
        assertThrows(ParamException.class, () -> RemoteBulkWriterParam.newBuilder()
                .withConnectParam(buildConnectParam())
                .withRemotePath("/tmp/remote_writer")
                .build());
    }

    @Test
    void testBuildRejectsNullConnectParam() {
        assertThrows(ParamException.class, () -> RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/remote_writer")
                .build());
    }
}
