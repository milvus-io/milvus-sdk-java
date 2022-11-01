package com.zilliz.milvustest.rbac;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.role.*;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;

/**
 * @Author yongpeng.li @Date 2022/9/20 19:40
 */
@Epic("Role")
@Feature("GrantRolePrivilege")
public class GrantRolePrivilegeTest extends BaseTest {
  String object;
  String objectName;
  String privilege;
  String collectionName;

  @DataProvider(name = "providerPrivilegeList")
  public Object[][] providerPrivilegeList() {
    File jsonFile=new File("./src/test/java/resources/testdata/privilege.json");
    String str = FileUtils.getStr(jsonFile);
    JSONArray jsonArray = JSONObject.parseArray(str);
    Object[][] objects=new Object[jsonArray.size()][3];
    for (int i = 0; i < jsonArray.size(); i++) {
      objects[i][0]=((JSONObject)jsonArray.get(i)).getString("object");
      objects[i][1]=((JSONObject)jsonArray.get(i)).getString("objectName");
      objects[i][2]=((JSONObject)jsonArray.get(i)).getString("privilege");
    }

    return objects;
  }

  @BeforeClass
  public void initTestData() {
    milvusClient.createRole(
        CreateRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
    milvusClient.addUserToRole(AddUserToRoleParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
            .withUserName(CommonData.defaultUserName).build());
  }

  @AfterMethod
  public void revokeRolePrivilege() {
    System.out.println("after:" + privilege);
    milvusClient.revokeRolePrivilege(
        RevokeRolePrivilegeParam.newBuilder()
            .withRoleName(CommonData.defaultRoleName)
            .withObject(object)
            .withObjectName(objectName)
            .withPrivilege(privilege)
            .build());
  }

  @AfterClass
  public void removeTestData() {
    milvusClient.removeUserFromRole(
            RemoveUserFromRoleParam.newBuilder()
                    .withRoleName(CommonData.defaultRoleName)
                    .withUserName(CommonData.defaultUserName)
                    .build());
    milvusClient.dropRole(
        DropRoleParam.newBuilder().withRoleName(CommonData.defaultRoleName).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Grant role privilege",dataProvider = "providerPrivilegeList",
      groups = {"Smoke"})
  public void grantRolePrivilegeTest(String object,String objectName,String privilege) {
    this.object=object;
    this.objectName=objectName;
    this.privilege=privilege;
    R<RpcStatus> rpcStatusR =
        milvusClient.grantRolePrivilege(
            GrantRolePrivilegeParam.newBuilder()
                .withRoleName(CommonData.defaultRoleName)
                .withObject(object)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Grant role privilege")
  public void grantCreateCollectionPrivilege() {
    object = "Global";
    objectName = "*";
    privilege = "CreateCollection";
    R<RpcStatus> rpcStatusR =
        milvusClient.grantRolePrivilege(
            GrantRolePrivilegeParam.newBuilder()
                .withRoleName(CommonData.defaultRoleName)
                .withObject(object)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    MilvusServiceClient milvusServiceClient = CommonFunction.newMilvusClient(CommonData.defaultUserName, CommonData.defaultPassword);
    collectionName = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("book_id")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(true)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultVectorField)
                    .withDataType(DataType.FloatVector)
                    .withDimension(128)
                    .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .build();
    R<RpcStatus> collection = milvusServiceClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().intValue(),0);
    R<RpcStatus> rpcStatusR1 = milvusServiceClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    Assert.assertTrue(rpcStatusR1.getMessage().contains("permission deny"));
  }
}
