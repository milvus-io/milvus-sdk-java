package com.zilliz.milvustest.credential;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.ListCredUsersResponse;
import io.milvus.param.R;
import io.milvus.param.credential.ListCredUsersParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Credential")
@Feature("ListCredUser")
public class ListCredUsersTest extends BaseTest {
  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "List all user names")
  public void listCredUsersTest() {
    R<ListCredUsersResponse> listCredUsersResponseR =
        milvusClient.listCredUsers(ListCredUsersParam.newBuilder().build());
    Assert.assertEquals(listCredUsersResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(listCredUsersResponseR.getData().getUsernamesList().size() > 1);
    Assert.assertTrue(
        listCredUsersResponseR.getData().getUsernamesList().contains(CommonData.defaultUserName));
    System.out.println(listCredUsersResponseR.getData());
  }
}
