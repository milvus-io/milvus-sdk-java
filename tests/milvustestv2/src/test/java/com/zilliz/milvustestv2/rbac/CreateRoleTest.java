package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 10:55
 */
public class CreateRoleTest extends BaseTest {

    @AfterClass(alwaysRun = true)
    public void deleteTestData(){
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @Test(description = "create role",groups = {"Smoke"})
    public void createRole(){
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
        List<String> strings = milvusClientV2.listRoles();
        Assert.assertTrue(strings.contains(CommonData.roleName));
    }

}
