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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li @Date 2022/9/20 17:49
 */
@Epic("Role")
@Feature("DropRole")
public class DropRoleTest extends BaseTest {

  @BeforeClass
  public void initCreateRole() {
    milvusClient.createRole(CreateRoleParam.newBuilder().withRoleName("newRoleName").build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Drop role",
      groups = {"Smoke"})
  public void dropRole() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropRole(DropRoleParam.newBuilder().withRoleName("newRoleName").build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
