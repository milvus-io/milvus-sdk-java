package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.role.AddUserToRoleParam;
import io.milvus.param.role.CreateRoleParam;
import io.milvus.param.role.DropRoleParam;
import io.milvus.param.role.RemoveUserFromRoleParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/20 18:14
 */
@Epic("Role")
@Feature("RemoveUserFromRole")
public class RemoveUserFromRoleTest extends BaseTest {
  @BeforeClass
  public void initTestData() {
    milvusClient.createRole(
        CreateRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
    milvusClient.addUserToRole(
        AddUserToRoleParam.newBuilder()
            .withUserName(CommonData.defaultUserName)
            .withRoleName(CommonData.defaultRoleName)
            .build());
  }

  @AfterClass
  public void removeRole() {
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Remove user from  role",
      groups = {"Smoke"})
  public void removeUserFromRoleTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.removeUserFromRole(
            RemoveUserFromRoleParam.newBuilder()
                .withRoleName(CommonData.defaultRoleName)
                .withUserName(CommonData.defaultUserName)
                .build());
    System.out.println(rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
