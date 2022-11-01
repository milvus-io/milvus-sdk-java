package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.SelectUserResponse;
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
 * @Author yongpeng.li @Date 2022/9/20 19:21
 */
@Epic("Role")
@Feature("SelectUser")
public class SelectUserTest extends BaseTest {
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
      description = "Select user",
      groups = {"Smoke"})
  public void selectUserWithRoleInfo() {
    R<SelectUserResponse> selectUserResponseR =
        milvusClient.selectUser(
            SelectUserParam.newBuilder()
                .withUserName(CommonData.defaultUserName)
                .withIncludeRoleInfo(true)
                .build());
    Assert.assertEquals(selectUserResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(
        selectUserResponseR.getData().getResults(0).getUser().getName(),
        CommonData.defaultUserName);
    Assert.assertEquals(
        selectUserResponseR.getData().getResults(0).getRoles(0).getName(),
        CommonData.defaultRoleName);
  }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "Select user",
            groups = {"Smoke"})
    public void selectUserWithoutRoleInfo() {
        R<SelectUserResponse> selectUserResponseR =
                milvusClient.selectUser(
                        SelectUserParam.newBuilder()
                                .withUserName(CommonData.defaultUserName)
                                .withIncludeRoleInfo(false)
                                .build());
        Assert.assertEquals(selectUserResponseR.getStatus().intValue(), 0);
        Assert.assertEquals(
                selectUserResponseR.getData().getResults(0).getUser().getName(),
                CommonData.defaultUserName);
    }
}
