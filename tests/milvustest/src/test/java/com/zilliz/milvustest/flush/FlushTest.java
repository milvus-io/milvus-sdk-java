package com.zilliz.milvustest.flush;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.FlushResponse;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Epic("Flush")
@Feature("Flush")
public class FlushTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "flush collection sync ",groups = {"Smoke"})
  public void flushCollectionSync() {
    List<String> collectionName =
        new ArrayList<String>() {
          {
            add(CommonData.defaultCollection);
          }
        };
    R<FlushResponse> flushResponseR =
        milvusClient.flush(
            FlushParam.newBuilder()
                .withCollectionNames(collectionName)
                .withSyncFlush(Boolean.TRUE)
                .build());
    Assert.assertEquals(flushResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(flushResponseR.getData().containsCollSegIDs(CommonData.defaultCollection));
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "flush collection async", enabled = true)
  public void flushCollectionAsync() {
    List<String> collectionName =
        new ArrayList<String>() {
          {
            add(CommonData.defaultCollection);
          }
        };
    R<FlushResponse> flushResponseR =
        milvusClient.flush(
            FlushParam.newBuilder()
                .withCollectionNames(collectionName)
                .withSyncFlushWaitingTimeout(30L)
                .withSyncFlushWaitingInterval(500L)
                .withSyncFlush(Boolean.FALSE)
                .build());
    Assert.assertEquals(flushResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(flushResponseR.getData().containsCollSegIDs(CommonData.defaultCollection));
  }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "flush collection with nonexistent collection")
    public void flushCollectionWithNonexistentCollection() {
        List<String> collectionName =
                new ArrayList<String>() {
                    {
                        add("NonexistentCollection");
                    }
                };
        R<FlushResponse> flushResponseR =
                milvusClient.flush(
                        FlushParam.newBuilder()
                                .withCollectionNames(collectionName)
                                .build());
        Assert.assertEquals(flushResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(
        flushResponseR.getData().getStatus().getReason().contains("can\'t find collection"));
    }
}
