package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author yongpeng.li
 * @Date 2024/2/28 15:59
 */
public class RevokePrivilegeTest extends BaseTest {

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(CommonData.roleName).build());
        milvusClientV2.grantPrivilege(GrantPrivilegeReq.builder()
                .roleName(CommonData.roleName)
                .privilege("DescribeCollection")
                .objectName("*")
                .objectType("Global")
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData() {
        milvusClientV2.dropRole(DropRoleReq.builder().roleName(CommonData.roleName).build());
    }

    @Test(description = "Revoke privilege", groups = {"Smoke"})
    public void revokePrivilege() {
        milvusClientV2.revokePrivilege(RevokePrivilegeReq.builder()
                .roleName(CommonData.roleName)
                .privilege("DescribeCollection")
                .objectName("*")
                .objectType("Global")
                .build());
        DescribeRoleResp describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                .roleName(CommonData.roleName).build());
        List<String> collect = describeRoleResp.getGrantInfos().stream().map(DescribeRoleResp.GrantInfo::getPrivilege).collect(Collectors.toList());
        Assert.assertFalse(collect.contains("DescribeCollection"));
    }


}
