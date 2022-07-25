package com.zilliz.milvustest.other;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.grpc.GetQuerySegmentInfoResponse;
import io.milvus.grpc.QuerySegmentInfo;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.milvus.param.control.LoadBalanceParam;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Balance")
@Feature("LoadBalance")
public class LoadBalanceTest extends BaseTest {
  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/356")
  @Test(description = "Moves segment from a query node to another to keep the load balanced.",groups = {"Smoke"})
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
    R<GetQuerySegmentInfoResponse> responseR =
            milvusClient.getQuerySegmentInfo(
                    GetQuerySegmentInfoParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .build());
    System.out.println("1:"+responseR.getData());
    List<QuerySegmentInfo> infosList = responseR.getData().getInfosList();
    R<RpcStatus> rpcStatusR = milvusClient.loadBalance(
            LoadBalanceParam.newBuilder()
                    .withSegmentIDs(Arrays.asList(infosList.get(0).getSegmentID()))
                    .withDestinationNodeID(Arrays.asList(infosList.get(0).getNodeIds(0)))
                    .withSourceNodeID(infosList.get(1).getNodeIds(0))
                    .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
    R<GetQuerySegmentInfoResponse> responseR2 =
            milvusClient.getQuerySegmentInfo(
                    GetQuerySegmentInfoParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .build());
    System.out.println("2:"+responseR2.getData());
  }
}
