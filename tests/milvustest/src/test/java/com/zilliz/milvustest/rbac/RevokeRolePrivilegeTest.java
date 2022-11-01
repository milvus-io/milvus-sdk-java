package com.zilliz.milvustest.rbac;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.role.CreateRoleParam;
import io.milvus.param.role.DropRoleParam;
import io.milvus.param.role.GrantRolePrivilegeParam;
import io.milvus.param.role.RevokeRolePrivilegeParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/21 10:55
 */
@Epic("Role")
@Feature("RevokeRolePrivilege")
public class RevokeRolePrivilegeTest extends BaseTest {
  @BeforeClass
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
    @AfterClass
    public void removeTestData(){
        milvusClient.dropRole(DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());

    }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Revoke role privilege",
      groups = {"Smoke"})
  public void revokeRolePrivilegeTest() {
      R<RpcStatus> rpcStatusR = milvusClient.revokeRolePrivilege(RevokeRolePrivilegeParam.newBuilder()
              .withRoleName(CommonData.defaultRoleName)
              .withObject("Global")
              .withObjectName("*")
              .withPrivilege("DescribeCollection")
              .build());
      Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
      Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
