package com.zilliz.milvustest.common;

import com.zilliz.milvustest.MilvustestApplication;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.credential.DeleteCredentialParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest(classes = MilvustestApplication.class)
@WebAppConfiguration
public class BaseCloudTest extends AbstractTestNGSpringContextTests {
//  public static final Logger logger= LoggerFactory.getLogger(BaseCloudTest.class);
  public static final MilvusServiceClient milvusCloudClient =
          new MilvusServiceClient(
                  ConnectParam.newBuilder()
                          .withHost(
                                  System.getProperty("milvusCloudHost") == null
                                          ? PropertyFilesUtil.getRunValue("milvusCloudHost")
                                          : System.getProperty("milvusCloudHost"))
                          .withPort(
                                  Integer.parseInt(
                                          System.getProperty("milvusCloudPort") == null
                                                  ? PropertyFilesUtil.getRunValue("milvusCloudPort")
                                                  : System.getProperty("milvusCloudPort")))
                          .withAuthorization(PropertyFilesUtil.getRunValue("milvusCloudName"),
                                  PropertyFilesUtil.getRunValue("milvusCloudPassword"))
                          .withSecure(true)
                          .build());

//  @BeforeSuite(alwaysRun = true)
  public void initCollection() {
    logger.info(
        "**************************************************BeforeSuit**********************");
    initEvn();
    // check collection is existed
    List<String> collections =
        new ArrayList<String>() {
          {
            add(CommonData.defaultCollection);
            add(CommonData.defaultBinaryCollection);
            add(CommonData.defaultStringPKCollection);
            add(CommonData.defaultStringPKBinaryCollection);
          }
        };
    checkCollection(collections);
    // create database
    milvusCloudClient.createDatabase(CreateDatabaseParam.newBuilder().withDatabaseName(CommonData.databaseName1).build());
  }

//  @AfterSuite(alwaysRun = true)
  public void cleanTestData() {
    logger.info(
        "**************************************************AfterSuit**********************");
    logger.info("drop Default Collection");
    milvusCloudClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    milvusCloudClient.dropCollection(
        DropCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .build());
    milvusCloudClient.dropCollection(
            DropCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .build());
    milvusCloudClient.dropCollection(
            DropCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .build());
    milvusCloudClient.dropAlias(DropAliasParam.newBuilder().withAlias(CommonData.defaultAlias).build());
    milvusCloudClient.dropAlias(DropAliasParam.newBuilder().withAlias(CommonData.defaultBinaryAlias).build());
    milvusCloudClient.dropAlias(DropAliasParam.newBuilder().withAlias(CommonData.defaultStringPKAlias).build());
    milvusCloudClient.dropAlias(DropAliasParam.newBuilder().withAlias(CommonData.defaultStringPKBinaryAlias).build());
    logger.info("delete Default Credential:" + CommonData.defaultUserName);
    milvusCloudClient.deleteCredential(
        DeleteCredentialParam.newBuilder().withUsername(CommonData.defaultUserName).build());
    milvusCloudClient.dropDatabase(DropDatabaseParam.newBuilder()
            .withDatabaseName(CommonData.databaseName1).build());
    milvusCloudClient.close();
  }

  public void initEvn() {
    logger.info("Initializing the Environment");
    // delete allure-result
    logger.info("Deletes all history json in the  allure-result folder");
    MathUtil.delAllFile("allure-results");
    // write environment for allure
    String filename = "./allure-results/environment.properties";
    Path path = Paths.get(filename);
    String contentStr =
        "milvus.url="
            + PropertyFilesUtil.getRunValue("milvusHost")
            + "\n"
            + "milvus.version="
            + PropertyFilesUtil.getRunValue("milvusV")
            + "\n"
            + "milvus-jdk-java.version="
            + PropertyFilesUtil.getRunValue("milvusJdkJavaV");
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write(contentStr);
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  public void checkCollection(List<String> collections) {
    collections.forEach(
        x -> {
          R<Boolean> booleanR =
              milvusCloudClient.hasCollection(
                  HasCollectionParam.newBuilder().withCollectionName(x).build());
          if (booleanR.getData()) {
            milvusCloudClient.dropCollection(
                DropCollectionParam.newBuilder().withCollectionName(x).build());
          }
        });
  }
}
