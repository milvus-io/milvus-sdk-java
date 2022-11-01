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
 * @Author yongpeng.li @Date 2022/9/21 14:02
 */
@Epic("Role")
@Feature("SelectGrantForRoleAndObject")
public class SelectGrantForRoleAndObject extends BaseTest {
  @BeforeClass
  public void initTestData() {
    milvusClient.createRole(
        CreateRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
    milvusClient.grantRolePrivilege(
        GrantRolePrivilegeParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
            .withObject("Collection")
            .withObjectName(CommonData.defaultCollection)
            .withPrivilege("Load")
            .build());
  }

  @AfterClass
  public void removeTestData() {
    milvusClient.revokeRolePrivilege(
        RevokeRolePrivilegeParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
            .withObject("Collection")
            .withObjectName(CommonData.defaultCollection)
            .withPrivilege("Load")
            .build());
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Select grant for role and object",
      groups = {"Smoke"})
  public void selectGrantForRoleAndObject() {
      R<SelectGrantResponse> selectGrantResponseR = milvusClient.selectGrantForRoleAndObject(
              SelectGrantForRoleAndObjectParam.newBuilder()
                      .withRoleName(CommonData.defaultRoleName)
                      .withObject("Collection")
                      .withObjectName(CommonData.defaultCollection)
                      .build());
      Assert.assertEquals(selectGrantResponseR.getStatus().intValue(), 0);
      Assert.assertEquals(
              selectGrantResponseR.getData().getEntities(0).getRole().getName(),
              CommonData.defaultRoleName);
      Assert.assertEquals(
              selectGrantResponseR.getData().getEntities(0).getObject().getName(), "Collection");
      Assert.assertEquals(selectGrantResponseR.getData().getEntities(0).getObjectName(), CommonData.defaultCollection);
  }
}
