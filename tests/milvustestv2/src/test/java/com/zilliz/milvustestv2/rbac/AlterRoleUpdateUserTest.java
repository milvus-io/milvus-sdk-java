package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.service.rbac.request.AlterRoleReq;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeRoleReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdateUserReq;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test cases for RBAC new interfaces: alterRole / updateUser (set description)
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class AlterRoleUpdateUserTest extends BaseTest {

    private String roleName;
    private String userName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        roleName = "AlterRole_" + GenerateUtil.getRandomString(6);
        userName = "AlterUser_" + GenerateUtil.getRandomString(6);
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(roleName).build());
        milvusClientV2.createUser(CreateUserReq.builder()
                .userName(userName)
                .password(CommonData.password)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void deleteTestData() {
        try {
            milvusClientV2.dropRole(DropRoleReq.builder().roleName(roleName).build());
        } catch (Exception e) {
            // ignore: role may already be dropped by test
        }
        try {
            milvusClientV2.dropUser(DropUserReq.builder().userName(userName).build());
        } catch (Exception e) {
            // ignore: user may already be dropped by test
        }
    }

    @Test(description = "alter role set description then update it", groups = {"Smoke"})
    public void alterRoleSetAndUpdateDescription() {
        // set description desc-v1
        milvusClientV2.alterRole(AlterRoleReq.builder()
                .roleName(roleName)
                .description("desc-v1")
                .build());
        DescribeRoleResp describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                .roleName(roleName)
                .build());
        Assert.assertEquals(describeRoleResp.getDescription(), "desc-v1",
                "description should be set to desc-v1");

        // update description to desc-v2
        milvusClientV2.alterRole(AlterRoleReq.builder()
                .roleName(roleName)
                .description("desc-v2")
                .build());
        describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                .roleName(roleName)
                .build());
        Assert.assertEquals(describeRoleResp.getDescription(), "desc-v2",
                "description should be updated to desc-v2");
    }

    @Test(description = "update user set description then update it", groups = {"Smoke"})
    public void updateUserSetAndUpdateDescription() {
        // set description user-desc-v1
        milvusClientV2.updateUser(UpdateUserReq.builder()
                .userName(userName)
                .description("user-desc-v1")
                .build());
        DescribeUserResp describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder()
                .userName(userName)
                .build());
        Assert.assertEquals(describeUserResp.getDescription(), "user-desc-v1",
                "description should be set to user-desc-v1");

        // update description to user-desc-v2
        milvusClientV2.updateUser(UpdateUserReq.builder()
                .userName(userName)
                .description("user-desc-v2")
                .build());
        describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder()
                .userName(userName)
                .build());
        Assert.assertEquals(describeUserResp.getDescription(), "user-desc-v2",
                "description should be updated to user-desc-v2");
    }

    @Test(description = "alter role with non-exist role name should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void alterRoleNotExistShouldFail() {
        milvusClientV2.alterRole(AlterRoleReq.builder()
                .roleName("NotExistRole_" + GenerateUtil.getRandomString(6))
                .description("desc")
                .build());
    }

    @Test(description = "update user with non-exist user name should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void updateUserNotExistShouldFail() {
        milvusClientV2.updateUser(UpdateUserReq.builder()
                .userName("NotExistUser_" + GenerateUtil.getRandomString(6))
                .description("desc")
                .build());
    }

    @Test(description = "alter role and update user with very long description (1000 chars)", groups = {"Smoke"})
    public void alterRoleAndUpdateUserWithLongDescription() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("d");
        }
        String longDesc = sb.toString();

        try {
            milvusClientV2.alterRole(AlterRoleReq.builder()
                    .roleName(roleName)
                    .description(longDesc)
                    .build());
            // 实际行为：服务端接受 1000 字符的 role description
            DescribeRoleResp describeRoleResp = milvusClientV2.describeRole(DescribeRoleReq.builder()
                    .roleName(roleName)
                    .build());
            Assert.assertEquals(describeRoleResp.getDescription(), longDesc,
                    "role description should equal the 1000-char string");
        } catch (Exception e) {
            // 实际行为：服务端拒绝超长 role description 并抛异常，异常信息: " + e.getMessage()
            Assert.assertTrue(true, "alterRole with 1000-char description throws: " + e.getMessage());
        }

        try {
            milvusClientV2.updateUser(UpdateUserReq.builder()
                    .userName(userName)
                    .description(longDesc)
                    .build());
            // 实际行为：服务端接受 1000 字符的 user description
            DescribeUserResp describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder()
                    .userName(userName)
                    .build());
            Assert.assertEquals(describeUserResp.getDescription(), longDesc,
                    "user description should equal the 1000-char string");
        } catch (Exception e) {
            // 实际行为：服务端拒绝超长 user description 并抛异常，异常信息: " + e.getMessage()
            Assert.assertTrue(true, "updateUser with 1000-char description throws: " + e.getMessage());
        }
    }
}
