package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.ImportResponse;
import io.milvus.grpc.ListImportTasksResponse;
import io.milvus.param.R;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.ListBulkInsertTasksParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/27 18:08
 */
@Epic("Import")
@Feature("ListImportTasks")
public class ListBulkInsertTasksTest extends BaseTest {
  @BeforeClass(description = "init bulk load task",alwaysRun = true)
  public void bulkLoad() {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
                BulkInsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("rowJson0.json")
                .build());
    logger.info("importResponseR:"+importResponseR);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "List import tasks Test",
      groups = {"Smoke"})
  public void ListImportTasksTest() {
    R<ListImportTasksResponse> listImportTasksResponseR =
        milvusClient.listBulkInsertTasks(ListBulkInsertTasksParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withLimit(1).build());
    Assert.assertEquals(listImportTasksResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(listImportTasksResponseR.getData().getTasksList().size() == 1);
  }
}
