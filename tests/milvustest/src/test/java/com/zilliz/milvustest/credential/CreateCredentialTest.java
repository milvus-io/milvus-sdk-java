package com.zilliz.milvustest.credential;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Credential")
@Feature("CreateCredential")
public class CreateCredentialTest extends BaseTest {
  private String username;
  private String password;

  @BeforeClass
  public void initCredentialInfo() {
    username = "user_" + MathUtil.getRandomString(5);
    password = "Pawd_" + MathUtil.getRandomString(5);
  }

  @AfterClass
  public void deleteCredentialInfo() {
    R<RpcStatus> rpcStatusR =
        milvusClient.deleteCredential(
            DeleteCredentialParam.newBuilder().withUsername(username).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Create credential using the given user and password.")
  public void createCredentialTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder()
                .withUsername(username)
                .withPassword(password)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "Create credential use repeated username",
      dependsOnMethods = "createCredentialTest")
  public void createCredentialRepeatedly() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder()
                .withUsername("root")
                .withPassword("Root123")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), -3);
    Assert.assertEquals(rpcStatusR.getException().getMessage(), "user already exists:root");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Check the rule of password ")
  public void checkTheRuleOfPassword() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder()
                .withUsername(CommonData.defaultUserName)
                .withPassword("r")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 5);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "The length of password must be great than 6 and less than 256 characters.");
  }

  @Severity(SeverityLevel.MINOR)
  @Test(
      description = "Create credential without username",
      expectedExceptions = ParamException.class)
  public void createCredentialWithoutUsername() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder().withPassword(CommonData.defaultPassword).build());
  }

  @Severity(SeverityLevel.MINOR)
  @Test(
      description = "Create credential without password",
      expectedExceptions = ParamException.class)
  public void createCredentialWithoutPassword() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder().withUsername(CommonData.defaultUserName).build());
  }
}
