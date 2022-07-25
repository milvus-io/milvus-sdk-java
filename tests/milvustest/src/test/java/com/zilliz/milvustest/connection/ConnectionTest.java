package com.zilliz.milvustest.connection;

import com.zilliz.milvustest.common.BaseTest;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Epic("Connection")
@Feature("Connect")
public class ConnectionTest extends BaseTest {

  @DataProvider(name = "connectParm")
  public Object[][] getHostAndProt() {
    return new Object[][] {
      {"in01-4fb62d5b6d1f782.aws-ap-southeast-1.vectordb-sit.zillizcloud.com", 19530}
    };
  }

  @Test(dataProvider = "connectParm",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void connectMilvus(String host, Integer prot) {
    MilvusServiceClient milvusClient =
        new MilvusServiceClient(ConnectParam.newBuilder().withHost(host).withPort(prot).build());
    System.out.println(milvusClient);
  }
}
