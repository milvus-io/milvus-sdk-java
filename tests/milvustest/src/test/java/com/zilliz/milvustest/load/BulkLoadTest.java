/*
package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.entity.FieldType;
import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.JacksonUtil;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.grpc.ImportResponse;
import io.milvus.param.R;
import io.milvus.param.dml.BulkloadParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.NonNull;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BulkLoadTest extends BaseTest {
    public int fileNums = 10;

    @BeforeClass(description = "initial test json files")
    public void generateJsonFiles() throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        List<FileBody> fileBodyList = CommonFunction.generateDefaultFileBody();
        String path = CommonData.defaultBulkLoadPath;

        FileUtils.generateMultipleFiles(Boolean.TRUE, 10, 128, fileBodyList, true
                , path, CommonData.defaultRowJson, "json", fileNums);
        FileUtils.generateMultipleFiles(Boolean.FALSE, 10, 128, fileBodyList, true
                , path, CommonData.defaultColJson, "json", fileNums);
        FileUtils.generateMultipleFiles(Boolean.TRUE, 0, 128, fileBodyList, true
                , path, "empty", "json", 1);

        List<FileBody> stringFileBody = CommonFunction.generateDefaultStringFileBody();
        FileUtils.generateMultipleFiles(Boolean.TRUE, 10, 128, stringFileBody, null
                , path, CommonData.defaultRowStrJson, "json", fileNums);
        FileUtils.generateMultipleFiles(Boolean.FALSE, 10, 128, stringFileBody, null
                , path, CommonData.defaultColStrJson, "json", 10);
        List<String> filenames = new ArrayList<>();
        for (int i = 0; i < fileNums; i++) {
            filenames.add(CommonData.defaultRowJson + i + ".json");
            filenames.add(CommonData.defaultColJson + i + ".json");
            filenames.add(CommonData.defaultRowStrJson + i + ".json");
            filenames.add(CommonData.defaultColStrJson + i + ".json");
        }
        filenames.add("empty0.json");

        FileUtils.multiFilesUpload(path, filenames, null);

    }

    @Epic("L0")
    @Test(description = "Import single row based json")
    public void loadSingleRowJsonTest() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection).withRowBased(true)
                .addFile("rowJson0.json")
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }

    @Epic("L0")
    @Test(description = "Import single column based json")
    public void loadSingleColJsonTest() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("colJson0.json").withRowBased(false)
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }

    @Epic("L0")
    @Test(description = "Import multiple row based json")
    public void loadMultipleRowJsonTest() {
        List<String> fileNames = new ArrayList<>();
        for (int i = 0; i < fileNums; i++) {
            fileNames.add(CommonData.defaultRowJson + i + ".json");
        }
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(fileNames)
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), fileNums);
    }

    @Epic("L0")
    @Test(description = "Import multiple column based json")
    public void loadMultipleColJsonTest() {
        List<String> fileNames = new ArrayList<>();
        for (int i = 0; i < fileNums; i++) {
            fileNames.add(CommonData.defaultColJson + i + ".json");
        }
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(fileNames)
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), fileNums);
    }

    @Feature("L1")
    @Test(description = "Import  row and column based json with add method")
    public void loadMixedJsonTest() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile(CommonData.defaultRowJson + 0 + ".json")
                .addFile(CommonData.defaultColJson + 0 + ".json")
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 2);
    }

    @Story("L2")
    @Test(description = "Import  empty row json")
    public void loadEmptyJsonTest() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("empty0.json")
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }

    @Story("L2")
    @Test(description = "Import  nonexistent  json")
    public void loadNonexistentJsonTest() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("nonexistent.json")
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }

    @Epic("L0")
    @Test(description = "Import single row-based json into String type field")
    public void loadSingleRowJsonIntoStringCollection() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultStringCollection)
                .addFile("rowStrJson0.json")
                .withRowBased(true)
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }

    @Epic("L0")
    @Test(description = "Import single col-based json into String type field")
    public void loadSingleColJsonIntoStringCollection() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("colStrJson0.json")
                .withRowBased(false)
                .build());
        Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    }
}
*/
