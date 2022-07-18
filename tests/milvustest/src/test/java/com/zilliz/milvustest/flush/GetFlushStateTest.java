package com.zilliz.milvustest.flush;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.GetFlushStateResponse;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.control.GetFlushStateParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Epic("Flush")
@Feature("GetFlushState")
public class GetFlushStateTest extends BaseTest {
  @DataProvider(name = "providerSegmentIds")
  public Object[][] providerSegmentId() {
    List<String> collections =
        new ArrayList<String>() {
          {
            add(CommonData.defaultCollection);
            add(CommonData.defaultBinaryCollection);
            add(CommonData.defaultStringPKCollection);
          }
        };
    R<FlushResponse> flushResponseR =
        milvusClient.flush(FlushParam.newBuilder().withCollectionNames(collections).build());
    Map<Long, String> maps = new HashMap<>();
    collections.forEach(
        x -> {
          List<Long> dataList = flushResponseR.getData().getCollSegIDsOrThrow(x).getDataList();
          dataList.forEach(
              y -> {
                maps.put(y, x);
              });
        });
    int i = 0;
    Object[][] objects = new Object[maps.size()][2];
    for (Long key : maps.keySet()) {
      objects[i][0] = key;
      objects[i][1] = maps.get(key);
      i++;
    }
    return objects;
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Get flush state of specified segments.", dataProvider = "providerSegmentIds")
  public void getFlushStateTest(Long segmentId, String collection) {
    R<GetFlushStateResponse> getFlushStateResponseR =
        milvusClient.getFlushState(GetFlushStateParam.newBuilder().addSegmentID(segmentId).build());
    Assert.assertEquals(getFlushStateResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(getFlushStateResponseR.getData().getFlushed());
    System.out.println("Collection-" + collection + ":" + getFlushStateResponseR.getData());
  }

    @Severity(SeverityLevel.MINOR)
    @Test(description = "Get flush state with nonexistent segmentId",expectedExceptions = ParamException.class)
    public void getFlushStateWithoutSegmentId() {
        R<GetFlushStateResponse> getFlushStateResponseR =
                milvusClient.getFlushState(GetFlushStateParam.newBuilder().build());
        Assert.assertEquals(getFlushStateResponseR.getStatus().intValue(), 1);
        Assert.assertEquals(getFlushStateResponseR.getException().getMessage(),"Segment id array cannot be empty");

    }
}
