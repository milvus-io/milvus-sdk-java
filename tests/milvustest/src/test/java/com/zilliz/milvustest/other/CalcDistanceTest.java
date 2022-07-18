package com.zilliz.milvustest.other;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.CalcDistanceResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.dml.CalcDistanceParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

@Epic("Other")
@Feature("CalcDistance")
public class CalcDistanceTest extends BaseTest {
  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Calculates the distance between the specified vectors.")
  public void calcDistanceTest() {
    List<List<Float>> leftList =
        Arrays.asList(Arrays.asList(0.1f, 0.2f), Arrays.asList(0.2f, 0.8f));
    List<List<Float>> rightList =
        Arrays.asList(Arrays.asList(0.3f, 0.4f), Arrays.asList(1.1f, 2.2f));
    R<CalcDistanceResults> calcDistanceResultsR =
        milvusClient.calcDistance(
            CalcDistanceParam.newBuilder()
                .withMetricType(MetricType.L2)
                .withVectorsLeft(leftList)
                .withVectorsRight(rightList)
                .build());
    Assert.assertEquals(calcDistanceResultsR.getStatus().intValue(), 0);
    System.out.println(calcDistanceResultsR.getData());
    Assert.assertTrue(calcDistanceResultsR.getData().getFloatDist().getData(0) > 0);
    Assert.assertEquals(calcDistanceResultsR.getData().getFloatDist().getDataCount(), 4);
  }


}
