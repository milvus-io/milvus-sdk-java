package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.DescribeRoleReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 14:22
 */
public class DescribeRoleTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData(){
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @Test(description = "describe role",groups = {"Smoke"})
    public void describeRoleTest(){
        DescribeRoleResp describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                .roleName(CommonData.roleName).build());
        System.out.println(describeRoleResp);
    }
}
