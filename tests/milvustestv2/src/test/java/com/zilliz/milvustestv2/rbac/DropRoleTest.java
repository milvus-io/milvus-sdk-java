package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 14:19
 */
public class DropRoleTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
    }


    @Test(description = "create role",groups = {"Smoke"})
    public void createRole(){
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
        List<String> strings = milvusClientV2.listRoles();
        Assert.assertFalse(strings.contains(CommonData.roleName));
    }
}
