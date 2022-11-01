package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.SelectRoleResponse;
import io.milvus.param.R;
import io.milvus.param.role.*;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/20 19:01
 */
@Epic("Role")
@Feature("SelectRole")
public class SelectRoleTest extends BaseTest {
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
    milvusClient.removeUserFromRole(RemoveUserFromRoleParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
            .withUserName(CommonData.defaultUserName)
            .build());
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Select role",
      groups = {"Smoke"})
  public void selectRoleWithUserInfo() {
    R<SelectRoleResponse> roleResponseR =
        milvusClient.selectRole(
            SelectRoleParam.newBuilder()
                .withRoleName(CommonData.defaultRoleName)
                .withIncludeUserInfo(true)
                .build());
    Assert.assertEquals(roleResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(
        roleResponseR.getData().getResults(0).getRole().getName(), CommonData.defaultRoleName);
    Assert.assertEquals(
        roleResponseR.getData().getResults(0).getUsers(0).getName(), CommonData.defaultUserName);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Select role",
      groups = {"Smoke"})
  public void selectRoleWithoutUserInfo() {
    R<SelectRoleResponse> roleResponseR =
        milvusClient.selectRole(
            SelectRoleParam.newBuilder()
                .withRoleName(CommonData.defaultRoleName)
                .withIncludeUserInfo(false)
                .build());
    Assert.assertEquals(roleResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(
        roleResponseR.getData().getResults(0).getRole().getName(), CommonData.defaultRoleName);
  }
}
