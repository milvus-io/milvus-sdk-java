package com.zilliz.milvustestv2.rbac;

import com.google.gson.*;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.FileUtils;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.PrivilegeEntity;
import io.milvus.param.role.RevokeRolePrivilegeParam;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import net.bytebuddy.implementation.auxiliary.PrivilegedMemberLookupAction;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 14:44
 */
public class GrantPrivilegeTest extends BaseTest {
    String object;
    String objectName;
    String privilege;

    @DataProvider(name = "providerPrivilegeList")
    public Object[][] providerPrivilegeList() {
        File jsonFile = new File("./src/test/resources/testdata/privilege.json");
        String str = FileUtils.getStr(jsonFile);
        JsonArray jsonArray = new Gson().fromJson(str, JsonArray.class);
        Object[][] objects = new Object[jsonArray.size()][3];
        for (int i = 0; i < jsonArray.size(); i++) {
            objects[i][0] = jsonArray.get(i).getAsJsonObject().get("object").getAsString();
            objects[i][1] = jsonArray.get(i).getAsJsonObject().get("objectName").getAsString();
            objects[i][2] = jsonArray.get(i).getAsJsonObject().get("privilege").getAsString();
        }
        return objects;
    }


    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData() {
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @AfterMethod(alwaysRun = true)
    public void revokeRolePrivilege() {
        milvusClientV2.revokePrivilege(RevokePrivilegeReq.builder()
                .roleName(CommonData.roleName)
                .privilege(privilege)
                .objectName(objectName)
                .objectType(object)
                .build());
    }

    @Test(description = "Grant privilege", groups = {"Smoke"}, dataProvider = "providerPrivilegeList")
    public void grantPrivilege(String object, String objectName, String privilege) {
        this.object=object;
        this.objectName=objectName;
        this.privilege=privilege;
        milvusClientV2.grantPrivilege(GrantPrivilegeReq.builder()
                .roleName(CommonData.roleName)
                .privilege(privilege)
                .objectName(objectName)
                .objectType(object)
                .build());
        DescribeRoleResp describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                .roleName(CommonData.roleName).build());
        Assert.assertEquals(describeRoleResp.getGrantInfos().get(0).getPrivilege(),privilege);
        Assert.assertEquals(describeRoleResp.getGrantInfos().get(0).getObjectName(),objectName);
        Assert.assertEquals(describeRoleResp.getGrantInfos().get(0).getObjectType(),object);
    }

}
