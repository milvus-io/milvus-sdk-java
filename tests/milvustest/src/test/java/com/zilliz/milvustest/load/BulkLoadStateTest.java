package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;

import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;

import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.response.GetCollStatResponseWrapper;
import io.milvus.response.QueryResultsWrapper;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @Author yongpeng.li @Date 2022/9/27 16:21
 */
@Epic("Import")
@Feature("GetImportState")
public class BulkLoadStateTest extends BaseTest {
  @BeforeClass
  public void generateJsonFiles(){
    BulkInsertTest bulkInsertTest=new BulkInsertTest();
    try {
      bulkInsertTest.generateJsonFiles();
    } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
      logger.error(e.getMessage());
    }
  }

  @DataProvider(name = "bigRowJsonTaskId")
  public Object[][] bigRowBasedTask()
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    String path = CommonData.defaultBulkLoadPath;
    /*List<FileBody> fileBodyList = CommonFunction.generateDefaultFileBody();
    Boolean aBoolean =
        FileUtils.generateMultipleFiles(
            Boolean.TRUE, 250000, 128, fileBodyList, true, path, "bigJson", "json", 1);
    System.out.println(aBoolean);*/
   /* long startUploadTime = System.currentTimeMillis();
    FileUtils.fileUploader(path, "bigJson0.json", null);
    long endUploadTime = System.currentTimeMillis();
    logger.info("upload to minio cost:" + (endUploadTime - startUploadTime) / 1000.0 + " seconds");
*/
    R<ImportResponse> bulkload =
        milvusClient.bulkInsert(
            BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("bigJson0.json")
                .build());
    ImportResponse data = bulkload.getData();
    return new Object[][] {{data.getTasksList().get(0)}};
  }

  @DataProvider(name = "singleRowBasedTaskId")
  public Object[][] singleRowBasedBulkLoad() {
    Object[][] objects = new Object[1][1];
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
            BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("rowJson0.json")
                .build());
    ImportResponse data = importResponseR.getData();
    Optional.ofNullable(data).ifPresent(x -> objects[0][0] = x.getTasks(0));
    return objects;
  }

  @DataProvider(name = "singleColBasedTaskId")
  public Object[][] singleColBasedBulkLoad() {
    Object[][] objects = new Object[1][1];
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
            BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("colJson0.json")
                .build());
    ImportResponse data = importResponseR.getData();
    Optional.ofNullable(data).ifPresent(x -> objects[0][0] = x.getTasks(0));
    return objects;
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Get bulk load state of  single row based json task",
      dataProvider = "singleRowBasedTaskId",
      groups = {"Smoke"})
  public void getSingleRowBaseJsonState(Long taskId) {
    R<GetImportStateResponse> bulkloadState =
        milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder().withTask(taskId).build());
    Assert.assertEquals(bulkloadState.getStatus().intValue(), 0);
//    Assert.assertEquals(bulkloadState.getData().getRowCount(), 10L);

    R<RpcStatus> rpcStatusR =
        milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    String query_Param = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withOutFields(outFields)
            .withExpr(query_Param)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    softAssert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    softAssert.assertEquals(queryResultsR.getData().getFieldsDataCount(), 2);
    softAssert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    System.out.println("query book_id:" + wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(
        "query word_count:" + wrapperQuery.getFieldWrapper("word_count").getFieldData());
    softAssert.assertAll();
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Get bulk load state of  single column based json task",
      dataProvider = "singleColBasedTaskId")
  public void getSingleColBaseJsonState(Long taskId) {
    R<GetImportStateResponse> bulkloadState =
        milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder().withTask(taskId).build());
    Assert.assertEquals(bulkloadState.getStatus().intValue(), 0);
    Assert.assertEquals(bulkloadState.getData().getRowCount(), 10L);
    Assert.assertEquals(bulkloadState.getData().getState(), ImportState.ImportCompleted);

    R<RpcStatus> rpcStatusR =
        milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    String query_Param = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withOutFields(outFields)
            .withExpr(query_Param)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    softAssert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    softAssert.assertEquals(queryResultsR.getData().getFieldsDataCount(), 2);
    softAssert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    System.out.println("query book_id:" + wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(
        "query word_count:" + wrapperQuery.getFieldWrapper("word_count").getFieldData());
    softAssert.assertAll();
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Get big json import task", dataProvider = "bigRowJsonTaskId")
  public void getBigJsonState(Long taskId) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    ImportState state = ImportState.ImportStarted;
    while (!(state.equals(ImportState.ImportCompleted) || state.equals(ImportState.ImportFailed))) {
      R<GetImportStateResponse> bulkloadState =
          milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder().withTask(taskId).build());
      state = bulkloadState.getData().getState();
      Thread.sleep(1000L);
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        "minio import to milvus cost:"
            + (endTime - startTime) / 1000.0
            + " seconds,result:"
            + state);

    R<GetCollectionStatisticsResponse> respCollectionStatistics =
            milvusClient
                    .getCollectionStatistics( // Return the statistics information of the collection.
                            GetCollectionStatisticsParam.newBuilder()
                                    .withCollectionName(CommonData.defaultCollection)
                                    .withFlush(true)
                                    .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 0);
    GetCollStatResponseWrapper wrapperCollectionStatistics =
            new GetCollStatResponseWrapper(respCollectionStatistics.getData());
    Assert.assertTrue(wrapperCollectionStatistics.getRowCount() > 200000);
    System.out.println(wrapperCollectionStatistics);
  }
}
