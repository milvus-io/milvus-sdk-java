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
import io.milvus.bulkwriter.connect.S3ConnectParam;
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
import java.util.ArrayList;
import java.util.List;

@Tag("unit")
public class RemoteBulkWriterTest {

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

    private static RemoteBulkWriterParam buildParam(String remotePath) {
        S3ConnectParam connectParam = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint("http://127.0.0.1:9000")
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .build();
        return RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withConnectParam(connectParam)
                .withRemotePath(remotePath)
                .withFileType(BulkFileType.JSON)
                .build();
    }

    /**
     * RemoteBulkWriter with a no-op upload callback so the writer lifecycle can be
     * exercised without reaching a real S3/MinIO endpoint.
     */
    private static class NoOpRemoteBulkWriter extends RemoteBulkWriter {
        private final List<String> committed = new ArrayList<>();

        NoOpRemoteBulkWriter(RemoteBulkWriterParam param) throws Exception {
            super(param);
        }

        @Override
        protected void callBack(List<String> fileList) {
            committed.addAll(fileList);
        }
    }

    @Test
    void testConstructAndAppend() throws Exception {
        String remotePath = "/tmp/remote_bulk_writer";
        try (NoOpRemoteBulkWriter writer = new NoOpRemoteBulkWriter(buildParam(remotePath))) {
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
        String remotePath = "/tmp/remote_bulk_writer_invalid";
        try (NoOpRemoteBulkWriter writer = new NoOpRemoteBulkWriter(buildParam(remotePath))) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 1L);
            // varchar_field is not nullable and has no default value -> must be provided
            Assertions.assertThrows(MilvusException.class, () -> writer.appendRow(row));
        }
    }

    @Test
    void testCommitEmptyWriterIsNoop() throws Exception {
        String remotePath = "/tmp/remote_bulk_writer_commit";
        try (NoOpRemoteBulkWriter writer = new NoOpRemoteBulkWriter(buildParam(remotePath))) {
            writer.commit(false);
            Assertions.assertTrue(writer.getBatchFiles().isEmpty());
        }
    }

    @Test
    void testCommitFlushesRowsAndInvokesCallback() throws Exception {
        String remotePath = "/tmp/remote_bulk_writer_flush";
        try (NoOpRemoteBulkWriter writer = new NoOpRemoteBulkWriter(buildParam(remotePath))) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 1L);
            row.addProperty("varchar_field", "value");
            writer.appendRow(row);
            writer.commit(false);
            Assertions.assertFalse(writer.committed.isEmpty());
            Assertions.assertEquals(1, writer.committed.size());
            Assertions.assertTrue(writer.getBatchFiles().isEmpty());
        }
    }

    @Test
    void testConstructorRejectsInvalidParam() {
        RemoteBulkWriterParam.Builder builder = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(buildSchema())
                .withRemotePath("/tmp/remote_bulk_writer_param");
        Assertions.assertThrows(ParamException.class, builder::build);
    }

    @Test
    void testAppendMultipleRows() throws Exception {
        String remotePath = "/tmp/remote_bulk_writer_multi";
        try (NoOpRemoteBulkWriter writer = new NoOpRemoteBulkWriter(buildParam(remotePath))) {
            for (int i = 0; i < 5; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", (long) i);
                row.addProperty("varchar_field", "v" + i);
                writer.appendRow(row);
            }
            Assertions.assertEquals(5L, writer.getTotalRowCount());
        }
    }
}
