package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.R;
import io.milvus.param.partition.ShowPartitionsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Epic("Partition")
@Feature("ShowPartitions")
public class ShowPartitionsTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "show partitions")
  public void showPartitionTest() {
    List<String> strings =
        new ArrayList<String>() {
          {
            add(CommonData.defaultPartition);
          }
        };
    R<ShowPartitionsResponse> showPartitionsResponseR =
        milvusClient.showPartitions(
            ShowPartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(strings)
                .build());
    Assert.assertEquals(showPartitionsResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(showPartitionsResponseR.getData().getPartitionIDsCount() > 1);

  }
}
