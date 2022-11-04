package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.SelectGrantResponse;
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
 * @Author yongpeng.li @Date 2022/9/21 11:40
 */
@Epic("Role")
@Feature("SelectGrantForRole")
public class SelectGrantForRoleTest extends BaseTest {
  @BeforeClass(alwaysRun=true)
  public void initTestData() {
    milvusClient.createRole(
        CreateRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
    milvusClient.grantRolePrivilege(
        GrantRolePrivilegeParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
                .withObject("Global")
                .withObjectName("*")
                .withPrivilege("DescribeCollection")
            .build());
  }

  @AfterClass(alwaysRun=true)
  public void removeTestData() {
    milvusClient.revokeRolePrivilege(
        RevokeRolePrivilegeParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
                .withObject("Global")
                .withObjectName("*")
                .withPrivilege("DescribeCollection")
            .build());
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Select grant for role",
      groups = {"Smoke"})
  public void selectGrantForRoleTest() {
    R<SelectGrantResponse> selectGrantResponseR =
        milvusClient.selectGrantForRole(
            SelectGrantForRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
    logger.info("selectGrantResponseR"+selectGrantResponseR);
    Assert.assertEquals(selectGrantResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(
        selectGrantResponseR.getData().getEntities(0).getRole().getName(),
        CommonData.defaultRoleName);
    Assert.assertTrue(
        selectGrantResponseR.getData().getEntitiesCount()>=1);
  }
}
