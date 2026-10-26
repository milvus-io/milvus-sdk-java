package com.zilliz.milvustestv2.bulk;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.common.utils.Float16Utils;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for BulkWriter V3 features (SDK 3.0.10): Text 字段写入、struct 内多向量类型
 * （binary/f16/bf16/int8 子向量）写入。本类只做本地文件写出校验（LocalBulkWriter appendRow/commit/
 * getBatchFiles），不做远端 import：commitImport/abortImport 依赖云端 import 服务，本环境（自建
 * Milvus standalone）没有该服务，故未覆盖。
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class BulkWriterV3Test extends BaseTest {

    private static final int DIM = 128;
    private static final int ROW_COUNT = 100;
    private static final int STRUCT_ROW_COUNT = 50;

    private final List<String> localPathsToClean = new ArrayList<>();

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        for (String path : localPathsToClean) {
            try {
                GenerateUtil.delAllFile(path);
            } catch (Exception e) {
                log.warn("cleanup local path " + path + " failed: " + e.getMessage());
            }
        }
    }

    @Test(description = "LocalBulkWriter writes Text field to PARQUET", groups = {"Smoke", "V3"})
    public void textFieldWriteParquet() throws Exception {
        String localPath = "/tmp/bulk_v3_text_parquet_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        List<List<String>> batchFiles = writeRows(buildTextSchema(), BulkFileType.PARQUET, localPath,
                generateTextRows(ROW_COUNT));
        assertBatchFiles(batchFiles);
    }

    @Test(description = "LocalBulkWriter writes Text field to JSON", groups = {"Smoke", "V3"})
    public void textFieldWriteJson() throws Exception {
        String localPath = "/tmp/bulk_v3_text_json_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        List<List<String>> batchFiles = writeRows(buildTextSchema(), BulkFileType.JSON, localPath,
                generateTextRows(ROW_COUNT));
        assertBatchFiles(batchFiles);
    }

    @Test(description = "LocalBulkWriter writes Text field to CSV", groups = {"Smoke", "V3"})
    public void textFieldWriteCsv() throws Exception {
        String localPath = "/tmp/bulk_v3_text_csv_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        List<List<String>> batchFiles = writeRows(buildTextSchema(), BulkFileType.CSV, localPath,
                generateTextRows(ROW_COUNT));
        assertBatchFiles(batchFiles);
    }

    @Test(description = "LocalBulkWriter writes struct with multi vector types (binary/f16/int8) to PARQUET", groups = {"Smoke", "V3"})
    public void structMultiVectorWriteParquet() throws Exception {
        String localPath = "/tmp/bulk_v3_struct_parquet_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        List<List<String>> batchFiles = writeRows(buildStructSchema(), BulkFileType.PARQUET, localPath,
                generateStructRows(STRUCT_ROW_COUNT));
        assertBatchFiles(batchFiles);
    }

    @Test(description = "Write number into Text field should fail", groups = {"Smoke", "V3"})
    public void textFieldWriteNumberShouldFail() throws Exception {
        String localPath = "/tmp/bulk_v3_text_bad_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(buildTextSchema())
                .withLocalPath(localPath)
                .withFileType(BulkFileType.PARQUET)
                .build();
        try (LocalBulkWriter writer = new LocalBulkWriter(param)) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 0L);
            row.addProperty("body", 12345); // Text 字段写数字，应抛类型不匹配异常
            Gson gson = new Gson();
            row.add("vector", gson.toJsonTree(CommonFunction.generateFloatVector(DIM)));
            writer.appendRow(row);
            Assert.fail("appendRow with number into Text field should throw");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Struct sub vector byte count mismatch with dim should fail", groups = {"Smoke", "V3"})
    public void structVectorDimMismatchShouldFail() throws Exception {
        String localPath = "/tmp/bulk_v3_struct_bad_" + GenerateUtil.getRandomString(6);
        localPathsToClean.add(localPath);
        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(buildStructSchema())
                .withLocalPath(localPath)
                .withFileType(BulkFileType.PARQUET)
                .build();
        try (LocalBulkWriter writer = new LocalBulkWriter(param)) {
            JsonObject row = new JsonObject();
            row.addProperty("id", 0L);
            JsonArray structArray = new JsonArray();
            JsonObject struct = new JsonObject();
            struct.addProperty("struct_int32", 0);
            // binary 子向量 dim=64 需要 8 字节，这里写 16 字节制造不匹配
            struct.add("struct_binary", bytesToJsonArray(new byte[16]));
            struct.add("struct_f16", bytesToJsonArray(
                    Float16Utils.f32VectorToFp16Buffer(CommonFunction.generateFloatVector(32)).array()));
            struct.add("struct_int8", bytesToJsonArray(new byte[16]));
            structArray.add(struct);
            row.add("struct_field", structArray);
            writer.appendRow(row);
            Assert.fail("appendRow with struct sub vector byte count mismatch should throw");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    /**
     * Text schema：Int64 pk + Text body + FloatVector(128)
     */
    private static CreateCollectionReq.CollectionSchema buildTextSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("body")
                .dataType(DataType.Text)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        return schema;
    }

    /**
     * struct schema：Int64 pk + struct array（Int32 子字段 + binary(64)/f16(32)/int8(16) 子向量）
     */
    private static CreateCollectionReq.CollectionSchema buildStructSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("struct_field")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName("struct_int32")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("struct_binary")
                        .dataType(DataType.BinaryVector)
                        .dimension(64)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("struct_f16")
                        .dataType(DataType.Float16Vector)
                        .dimension(32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("struct_int8")
                        .dataType(DataType.Int8Vector)
                        .dimension(16)
                        .build())
                .build());
        return schema;
    }

    /**
     * 生成含 Text 字段的行（body 为长文本）
     */
    private static List<JsonObject> generateTextRows(int count) {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            sb.append(GenerateUtil.getRandomString(50));
        }
        String longText = sb.toString();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (long) i);
            row.addProperty("body", longText + "_" + i);
            row.add("vector", gson.toJsonTree(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));
            rows.add(row);
        }
        return rows;
    }

    /**
     * 生成含 struct 数组的行：每行 2 个 struct 元素；
     * binary/f16/int8 子向量都写字节列表（JSON array of numbers），f16 用 Float16Utils 转换
     */
    private static List<JsonObject> generateStructRows(int count) {
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (long) i);
            JsonArray structArray = new JsonArray();
            for (int j = 0; j < 2; j++) {
                JsonObject struct = new JsonObject();
                struct.addProperty("struct_int32", i * 10 + j);
                // binary dim=64 -> 8 字节
                struct.add("struct_binary", bytesToJsonArray(CommonFunction.generateBinaryVector(64).array()));
                // f16 dim=32 -> 64 字节
                struct.add("struct_f16", bytesToJsonArray(
                        Float16Utils.f32VectorToFp16Buffer(CommonFunction.generateFloatVector(32)).array()));
                // int8 dim=16 -> 16 字节
                struct.add("struct_int8", bytesToJsonArray(CommonFunction.generateBinaryVector(16 * 8).array()));
                structArray.add(struct);
            }
            row.add("struct_field", structArray);
            rows.add(row);
        }
        return rows;
    }

    /**
     * LocalBulkWriter 写本地文件并 commit，返回 batchFiles
     */
    private static List<List<String>> writeRows(CreateCollectionReq.CollectionSchema schema, BulkFileType fileType,
                                                String localPath, List<JsonObject> rows) throws Exception {
        LocalBulkWriterParam param = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schema)
                .withLocalPath(localPath)
                .withFileType(fileType)
                .build();
        List<List<String>> batchFiles;
        try (LocalBulkWriter writer = new LocalBulkWriter(param)) {
            for (JsonObject row : rows) {
                writer.appendRow(row);
            }
            writer.commit(false);
            batchFiles = writer.getBatchFiles();
        }
        return batchFiles;
    }

    /**
     * 断言 batchFiles 非空且文件在本地存在
     */
    private static void assertBatchFiles(List<List<String>> batchFiles) {
        Assert.assertNotNull(batchFiles);
        Assert.assertFalse(batchFiles.isEmpty(), "batchFiles should not be empty");
        for (List<String> batch : batchFiles) {
            for (String filePath : batch) {
                Assert.assertTrue(new File(filePath).exists(), "batch file should exist: " + filePath);
            }
        }
        log.info("batchFiles: " + batchFiles);
    }

    private static JsonArray bytesToJsonArray(byte[] bytes) {
        JsonArray jsonArray = new JsonArray();
        for (byte b : bytes) {
            jsonArray.add(b);
        }
        return jsonArray;
    }
}
