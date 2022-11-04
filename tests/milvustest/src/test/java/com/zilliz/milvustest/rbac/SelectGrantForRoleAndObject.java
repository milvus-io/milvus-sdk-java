package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.SelectGrantResponse;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
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
      R<RpcStatus> newRole = milvusClient.createRole(
              CreateRoleParam.newBuilder().withRoleName("newRole").build());
      logger.info("newRole:"+newRole);
      R<RpcStatus> rpcStatusR = milvusClient.grantRolePrivilege(
              GrantRolePrivilegeParam.newBuilder()
                      .withRoleName("newRole")
                      .withObject("Collection")
                      .withObjectName(CommonData.defaultCollection)
                      .withPrivilege("Load")
                      .build());
      logger.info("rpcStatusR:"+rpcStatusR);
  }

  @AfterClass
  public void removeTestData() {
    milvusClient.revokeRolePrivilege(
        RevokeRolePrivilegeParam.newBuilder()
            .withRoleName("newRole")
            .withObject("Collection")
            .withObjectName(CommonData.defaultCollection)
            .withPrivilege("Load")
            .build());
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName("newRole").build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Select grant for role and object",
      groups = {"Smoke"})
  public void selectGrantForRoleAndObject() {
      R<SelectGrantResponse> selectGrantResponseR = milvusClient.selectGrantForRoleAndObject(
              SelectGrantForRoleAndObjectParam.newBuilder()
                      .withRoleName("newRole")
                      .withObject("Collection")
                      .withObjectName(CommonData.defaultCollection)
                      .build());
      logger.info("selectGrantResponseR"+selectGrantResponseR);
      Assert.assertEquals(selectGrantResponseR.getStatus().intValue(), 0);
      Assert.assertEquals(
              selectGrantResponseR.getData().getEntities(0).getRole().getName(),
              "newRole");
      Assert.assertEquals(
              selectGrantResponseR.getData().getEntities(0).getObject().getName(), "Collection");
      Assert.assertEquals(selectGrantResponseR.getData().getEntities(0).getObjectName(), CommonData.defaultCollection);
  }
}
