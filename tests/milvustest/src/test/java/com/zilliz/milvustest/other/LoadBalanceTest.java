package com.zilliz.milvustest.other;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.control.LoadBalanceParam;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Balance")
@Feature("LoadBalance")
public class LoadBalanceTest extends BaseTest {
  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Moves segment from a query node to another to keep the load balanced.")
  public void loadBalanceTest() {
    // insert
    List<InsertParam.Field> fields = CommonFunction.generateData(3000);
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withFields(fields)
            .build());
    // flush
    milvusClient.flush(
        FlushParam.newBuilder()
            .withCollectionNames(Arrays.asList(CommonData.defaultCollection))
            .withSyncFlush(true)
            .withSyncFlushWaitingInterval(500L)
            .withSyncFlushWaitingTimeout(30L)
            .build());
    // load
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withSyncLoad(true)
            .withSyncLoadWaitingInterval(500L)
            .withSyncLoadWaitingTimeout(30L)
            .build());
    // query Segment
    R<GetPersistentSegmentInfoResponse> responseR =
        milvusClient.getPersistentSegmentInfo(
            GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    milvusClient.loadBalance(
        LoadBalanceParam.newBuilder()
            .withSegmentIDs(Arrays.asList(434445867837030402L))
            .withDestinationNodeID(Arrays.asList(6L))
            .withSourceNodeID(3L)
            .build());
  }
}
