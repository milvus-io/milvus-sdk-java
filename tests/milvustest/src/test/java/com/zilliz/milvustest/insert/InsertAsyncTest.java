package com.zilliz.milvustest.insert;

import com.google.common.util.concurrent.ListenableFuture;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Epic("Index")
@Feature("InsertAsyncT")
public class InsertAsyncTest extends BaseTest {

  @Test(description = "Insert data Async")
  @Severity(SeverityLevel.BLOCKER)
  public void insertAsyncSuccess() throws ExecutionException, InterruptedException {
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    ListenableFuture<R<MutationResult>> rListenableFuture =
        milvusClient.insertAsync(
            InsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFields(fields)
                .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);
    System.out.println(mutationResultR.getData());
  }
}
