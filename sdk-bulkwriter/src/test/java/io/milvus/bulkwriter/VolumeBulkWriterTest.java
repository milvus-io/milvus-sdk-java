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

import com.google.gson.JsonObject;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.exception.MilvusException;
import io.milvus.exception.ParamException;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

@Tag("unit")
public class VolumeBulkWriterTest {

    @AfterAll
    static void cleanupLocalWriterDir() {
        File dir = new File("bulk_writer");
        if (dir.exists()) {
            deleteRecursively(dir);
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private static CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        return schema;
    }

    private static VolumeBulkWriterParam buildParam(String remotePath) {
        return VolumeBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath(remotePath)
                .withFileType(BulkFileType.JSON)
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("api-key")
                .withVolumeName("volume-1")
                .build();
    }

    private static class NoOpVolumeBulkWriter extends VolumeBulkWriter {
        NoOpVolumeBulkWriter(VolumeBulkWriterParam param) throws Exception {
            super(param);
        }

        @Override
        protected void callBack(java.util.List<String> fileList) {
            // no-op to avoid network upload
        }
    }

    @Test
    void testConstructAndAppend() throws Exception {
        String remotePath = "/tmp/volume_bulk_writer";
        try (NoOpVolumeBulkWriter writer = new NoOpVolumeBulkWriter(buildParam(remotePath))) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 1L);
            row.addProperty("varchar_field", "value");
            writer.appendRow(row);
            Assertions.assertEquals(1L, writer.getTotalRowCount());
            Assertions.assertTrue(writer.getDataPath().startsWith(remotePath));
            Assertions.assertTrue(writer.getBatchFiles().isEmpty());
        }
    }

    @Test
    void testAppendInvalidRowThrows() throws Exception {
        String remotePath = "/tmp/volume_bulk_writer_invalid";
        try (NoOpVolumeBulkWriter writer = new NoOpVolumeBulkWriter(buildParam(remotePath))) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 1L);
            Assertions.assertThrows(MilvusException.class, () -> writer.appendRow(row));
        }
    }

    @Test
    void testGetVolumeUploadResult() throws Exception {
        String remotePath = "/tmp/volume_bulk_writer_result";
        try (NoOpVolumeBulkWriter writer = new NoOpVolumeBulkWriter(buildParam(remotePath))) {
            io.milvus.bulkwriter.model.UploadFilesResult result = writer.getVolumeUploadResult();
            Assertions.assertEquals("volume-1", result.getVolumeName());
            Assertions.assertTrue(result.getPath().startsWith(remotePath));
        }
    }

    @Test
    void testConstructorRejectsMissingRequiredFields() {
        VolumeBulkWriterParam.Builder builder = VolumeBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/volume_bulk_writer_param");
        Assertions.assertThrows(ParamException.class, builder::build);
    }
}
