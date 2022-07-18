package com.zilliz.milvustest.credential;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Credential")
@Feature("DeleteCredential")
public class DeleteCredentialTest extends BaseTest {
  private String username;
  private String password;

  @BeforeClass
  public void initCredentialInfo() {
    username = "user_" + MathUtil.getRandomString(5);
    password = "Pawd_" + MathUtil.getRandomString(5);
    R<RpcStatus> rpcStatusR =
        milvusClient.createCredential(
            CreateCredentialParam.newBuilder()
                .withUsername(username)
                .withPassword(password)
                .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "delete credential by username")
  public void deleteCredential() {
    R<RpcStatus> rpcStatusR =
        milvusClient.deleteCredential(
            DeleteCredentialParam.newBuilder().withUsername(username).build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "delete credential repeatedly", dependsOnMethods = "deleteCredential")
  public void deleteCredentialRepeatedly() {
    R<RpcStatus> rpcStatusR =
        milvusClient.deleteCredential(
            DeleteCredentialParam.newBuilder().withUsername(username).build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "delete root credential ")
  public void deleteRootCredential() {
    R<RpcStatus> rpcStatusR =
        milvusClient.deleteCredential(
            DeleteCredentialParam.newBuilder().withUsername("root").build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), -3);
    Assert.assertEquals(rpcStatusR.getException().getMessage(), "user root cannot be deleted");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "delete credential nonexistent username")
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/308")
  public void deleteCredentialNonexistent() {
    R<RpcStatus> rpcStatusR =
        milvusClient.deleteCredential(
            DeleteCredentialParam.newBuilder().withUsername("nonexistent").build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
