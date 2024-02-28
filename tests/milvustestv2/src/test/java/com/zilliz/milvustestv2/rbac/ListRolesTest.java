package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 14:26
 */
public class ListRolesTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData(){
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @Test(description = "list roles",groups = {"Smoke"})
    public void listRolesTest(){
        List<String> strings = milvusClientV2.listRoles();
        Assert.assertTrue(strings.contains(CommonData.roleName));
    }

}
