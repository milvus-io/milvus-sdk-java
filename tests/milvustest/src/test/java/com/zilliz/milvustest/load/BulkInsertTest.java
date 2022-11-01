package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.util.FileUtils;
import io.milvus.grpc.GetImportStateResponse;
import io.milvus.grpc.ImportResponse;
import io.milvus.param.R;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author yongpeng.li @Date 2022/9/26 17:32
 */
@Epic("BulkInsert")
@Feature("BulkInsert")
public class BulkInsertTest extends BaseTest {
  public int fileNums = 10;



  @BeforeClass(description = "initial test json files")
  public void generateJsonFiles()
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    List<FileBody> fileBodyList = CommonFunction.generateDefaultFileBody();
    String path = CommonData.defaultBulkLoadPath;

    FileUtils.generateMultipleFiles(
        Boolean.TRUE,
        10,
        128,
        fileBodyList,
        true,
        path,
        CommonData.defaultRowJson,
        "json",
        fileNums);
    FileUtils.generateMultipleFiles(
        Boolean.FALSE,
        10,
        128,
        fileBodyList,
        true,
        path,
        CommonData.defaultColJson,
        "json",
        fileNums);
    FileUtils.generateMultipleFiles(
        Boolean.TRUE, 0, 128, fileBodyList, true, path, "empty", "json", 1);

    List<FileBody> stringFileBody = CommonFunction.generateDefaultStringFileBody();
    FileUtils.generateMultipleFiles(
        Boolean.TRUE,
        10,
        128,
        stringFileBody,
        null,
        path,
        CommonData.defaultRowStrJson,
        "json",
        fileNums);
    FileUtils.generateMultipleFiles(
        Boolean.FALSE,
        10,
        128,
        stringFileBody,
        null,
        path,
        CommonData.defaultColStrJson,
        "json",
        10);
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

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Import single row based json",
      groups = {"Smoke"})
  public void importSingleRowJsonTest() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
            BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(Arrays.asList("rowJson0.json"))
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksList().size(), 1);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Import single column based json",
      groups = {"Smoke"})
  public void importSingleColumnJsonTest() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(Arrays.asList("colJson0.json"))
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksList().size(), 1);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Import multiple row based json")
  public void importMultipleRowJsonTest() {
    List<String> fileNames = new ArrayList<>();
    for (int i = 0; i < fileNums; i++) {
      fileNames.add(CommonData.defaultRowJson + i + ".json");
    }
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(fileNames)
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksList().size(), fileNums);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Import multiple column based json")
  public void importMultipleColJsonTest() {
    List<String> fileNames = new ArrayList<>();
    for (int i = 0; i < fileNums; i++) {
      fileNames.add(CommonData.defaultColJson + i + ".json");
    }
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFiles(fileNames)
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksList().size(), 1);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Import multiple row based json with Add")
  public void importRowJsonUseAddTest() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile(CommonData.defaultRowJson + 0 + ".json")
                .addFile(CommonData.defaultRowJson + 1 + ".json")
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksCount(), 2);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Import multiple column based json")
  public void importMixedJsonTest() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile(CommonData.defaultRowJson + 0 + ".json")
                .addFile(CommonData.defaultColJson + 1 + ".json")
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
    Long taskId=importResponseR.getData().getTasksList().get(0);
    R<GetImportStateResponse> bulkloadState = milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder()
            .withTask(taskId).build());
    Assert.assertEquals(bulkloadState.getStatus().intValue(),0);
    Assert.assertEquals(bulkloadState.getData().getRowCount(),20L);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Import empty json")
  public void importEmptyJsonTest() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("empty0.json")
                .build());
    Assert.assertEquals(importResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(importResponseR.getData().getTasksCount(), 1);
  }

}
