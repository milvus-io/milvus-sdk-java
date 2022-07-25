package com.zilliz.milvustest.other;

import com.zilliz.milvustest.common.BaseTest;
import io.milvus.grpc.GetMetricsResponse;
import io.milvus.param.R;
import io.milvus.param.control.GetMetricsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Metrics")
@Feature("GetMetrics")
public class GetMetricsTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description =
          "Gets the runtime metrics information of Milvus, returns the result in .json format."
          ,groups = {"Smoke"})
  public void getMetricTest() {
    String param = "{\"metric_type\": \"system_info\"}";
    R<GetMetricsResponse> getMetricsResponseR =
        milvusClient.getMetrics(GetMetricsParam.newBuilder().withRequest(param).build());
    Assert.assertEquals(getMetricsResponseR.getStatus().intValue(), 0);
    System.out.println(getMetricsResponseR.getData());
    Assert.assertTrue(getMetricsResponseR.getData().getResponse().contains("nodes_info"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Gets the runtime metrics information with illegal param")
  public void getMetricByNodeInfo() {
    String param = "{\"metric_type\": \"node_id\"}";
    R<GetMetricsResponse> getMetricsResponseR =
        milvusClient.getMetrics(GetMetricsParam.newBuilder().withRequest(param).build());
    Assert.assertEquals(getMetricsResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(
        getMetricsResponseR
            .getException()
            .getMessage()
            .contains("sorry, but this metric type is not implemented"));
  }
}
