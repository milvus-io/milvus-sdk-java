package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.role.CreateRoleParam;
import io.milvus.param.role.DropRoleParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/20 17:24
 */
@Epic("Role")
@Feature("CreateRole")

public class CreateRoleTest extends BaseTest {
  @AfterClass
  public void removeTestData() {
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName("newRole").build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Create role",
      groups = {"Smoke"})
  public void createRole() {
    R<RpcStatus> role =
        milvusClient.createRole(
            CreateRoleParam.newBuilder().withRoleName("newRole").build());
    Assert.assertEquals(role.getStatus().intValue(), 0);
    Assert.assertEquals(role.getData().getMsg(), "Success");
  }
}
