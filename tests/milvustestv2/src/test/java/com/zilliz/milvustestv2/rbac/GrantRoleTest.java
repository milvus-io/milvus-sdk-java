package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 14:31
 */
public class GrantRoleTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
        milvusClientV2.createUser(CreateUserReq.builder().userName(CommonData.userName).password(CommonData.password).build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData(){
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
        milvusClientV2.dropUser(DropUserReq.builder().userName(CommonData.userName).build());
    }

    @Test(description = "grant role",groups = {"Smoke"})
    public void GrantRole(){
        milvusClientV2.grantRole(GrantRoleReq.builder()
                .userName(CommonData.userName)
                .roleName(CommonData.roleName)
                .build());
        DescribeUserResp describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder().userName(CommonData.userName).build());
        Assert.assertTrue(describeUserResp.getRoles().contains(CommonData.roleName));
    }

}
