package com.zilliz.milvustest.credential;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Credential")
@Feature("UpdateCredential")
public class UpdateCredentialTest extends BaseTest {
  private String username;
  private String password;

  @BeforeClass(alwaysRun = true)
  public void initCredentialInfo() {
    username = "user_" + MathUtil.getRandomString(5);
    password = "Pwd_" + MathUtil.getRandomString(5);
    milvusClient.createCredential(
        CreateCredentialParam.newBuilder().withUsername(username).withPassword(password).build());
  }

  @AfterClass(alwaysRun = true)
  public void deleteCredentialInfo() {
    milvusClient.deleteCredential(
        DeleteCredentialParam.newBuilder().withUsername(username).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Update credential using the given user and password",groups = {"Smoke"})
  public void updateCredentialTest() {
    String newPWD = "Pwd_" + MathUtil.getRandomString(6);
    R<RpcStatus> rpcStatusR =
        milvusClient.updateCredential(
            UpdateCredentialParam.newBuilder()
                .withUsername(username)
                .withOldPassword(password)
                .withNewPassword(newPWD)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Update root  credential")
  public void updateRootCredentialTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.updateCredential(
            UpdateCredentialParam.newBuilder()
                .withUsername("root")
                .withOldPassword("Milvus")
                .withNewPassword("Milvus")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Update credential with nonexistent username")
  public void updateCredentialNonexistentUserName() {
    R<RpcStatus> rpcStatusR =
        milvusClient.updateCredential(
            UpdateCredentialParam.newBuilder()
                .withUsername("nonexistent")
                .withOldPassword("nonexistent")
                .withNewPassword("nonexistent")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), -3);
    Assert.assertEquals(rpcStatusR.getException().getMessage(), "found no credential:nonexistent");
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Update credential using illegal password")
  public void updateCredentialWithIllegalPassword() {
    R<RpcStatus> rpcStatusR =
            milvusClient.updateCredential(
                    UpdateCredentialParam.newBuilder()
                            .withUsername(username)
                            .withOldPassword(password)
                            .withNewPassword("#$%^&")
                            .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 5);
    Assert.assertEquals(rpcStatusR.getException().getMessage(), "The length of password must be great than 6 and less than 256 characters.");
  }
}
