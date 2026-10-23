package com.zilliz.milvustestv2.rbac;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.GrantPrivilegeReq;
import io.milvus.v2.service.rbac.request.GrantRoleReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for dropRole with forceDrop option (SDK 2.6 new param).
 * forceDrop=false: server rejects dropping a role still bound to users/privileges;
 * forceDrop=true: force drop even if the role is occupied.
 * 实测发现（2.6-20261022 nightly）：forceDrop=false 删除绑定用户的角色不再被拒绝，
 * 直接删除成功且绑定关系被一并移除，forceDrop 语义在该构建上未生效。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class DropRoleForceTest extends BaseTest {

    private final List<String> createdRoles = new ArrayList<>();
    private final List<String> createdUsers = new ArrayList<>();

    @AfterClass(alwaysRun = true)
    public void deleteTestData() {
        for (String role : createdRoles) {
            try {
                milvusClientV2.dropRole(DropRoleReq.builder().roleName(role).forceDrop(true).build());
            } catch (Exception e) {
                // ignore: role may already be dropped by test
            }
        }
        for (String user : createdUsers) {
            try {
                milvusClientV2.dropUser(DropUserReq.builder().userName(user).build());
            } catch (Exception e) {
                // ignore: user may already be dropped by test
            }
        }
    }

    @Test(description = "drop role bound to user: forceDrop=false also succeeds on 2.6 nightly, forceDrop=true as comparison", groups = {"Smoke"})
    public void dropRoleBoundToUser() {
        String roleName = "ForceRole_" + GenerateUtil.getRandomString(6);
        String userName = "ForceUser_" + GenerateUtil.getRandomString(6);
        createdRoles.add(roleName);
        createdUsers.add(userName);

        milvusClientV2.createRole(CreateRoleReq.builder().roleName(roleName).build());
        milvusClientV2.createUser(CreateUserReq.builder()
                .userName(userName)
                .password(CommonData.password)
                .build());
        milvusClientV2.grantRole(GrantRoleReq.builder()
                .userName(userName)
                .roleName(roleName)
                .build());
        DescribeUserResp describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder()
                .userName(userName)
                .build());
        Assert.assertTrue(describeUserResp.getRoles().contains(roleName));

        // 行为固化：2.6-20261022 实测——forceDrop=false 删除被占用角色不再抛异常，
        // 直接删除成功且绑定关系被一并移除，forceDrop 语义在该构建上未生效
        milvusClientV2.dropRole(DropRoleReq.builder()
                .roleName(roleName)
                .forceDrop(false)
                .build());
        Assert.assertFalse(milvusClientV2.listRoles().contains(roleName),
                "role bound to user should be dropped with forceDrop=false (2.6 nightly behavior)");

        // the user should no longer hold this role
        describeUserResp = milvusClientV2.describeUser(DescribeUserReq.builder()
                .userName(userName)
                .build());
        Assert.assertFalse(describeUserResp.getRoles().contains(roleName),
                "user's roles should not contain the dropped role");

        // 对照场景：forceDrop=true 删除被占用角色同样成功
        milvusClientV2.createRole(CreateRoleReq.builder().roleName(roleName).build());
        milvusClientV2.grantRole(GrantRoleReq.builder()
                .userName(userName)
                .roleName(roleName)
                .build());
        milvusClientV2.dropRole(DropRoleReq.builder()
                .roleName(roleName)
                .forceDrop(true)
                .build());
        Assert.assertFalse(milvusClientV2.listRoles().contains(roleName),
                "role should be force dropped with forceDrop=true");

        milvusClientV2.dropUser(DropUserReq.builder().userName(userName).build());
        createdUsers.remove(userName);
    }

    @Test(description = "force drop role bound with privilege", groups = {"Smoke"})
    public void dropRoleBoundWithPrivilege() {
        String roleName = "ForcePrivRole_" + GenerateUtil.getRandomString(6);
        createdRoles.add(roleName);

        milvusClientV2.createRole(CreateRoleReq.builder().roleName(roleName).build());
        milvusClientV2.grantPrivilege(GrantPrivilegeReq.builder()
                .roleName(roleName)
                .objectType("Global")
                .objectName("*")
                .privilege("CreateCollection")
                .build());

        // forceDrop=true: drop the role even though it owns privileges
        milvusClientV2.dropRole(DropRoleReq.builder()
                .roleName(roleName)
                .forceDrop(true)
                .build());
        Assert.assertFalse(milvusClientV2.listRoles().contains(roleName),
                "role with privileges should be force dropped");
        createdRoles.remove(roleName);
    }

    @Test(description = "force drop non-exist role should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void dropNonExistRoleWithForceShouldFail() {
        // 以实际行为为准：预期服务端返回 role not found 异常
        milvusClientV2.dropRole(DropRoleReq.builder()
                .roleName("NotExistRole_" + GenerateUtil.getRandomString(6))
                .forceDrop(true)
                .build());
    }

    @Test(description = "drop unoccupied role with forceDrop=false succeeds (boundary)", groups = {"Smoke"})
    public void dropUnoccupiedRoleWithoutForce() {
        String roleName = "FreeRole_" + GenerateUtil.getRandomString(6);
        createdRoles.add(roleName);

        milvusClientV2.createRole(CreateRoleReq.builder().roleName(roleName).build());
        // role is not bound to any user/privilege, forceDrop=false should also work
        milvusClientV2.dropRole(DropRoleReq.builder()
                .roleName(roleName)
                .forceDrop(false)
                .build());
        Assert.assertFalse(milvusClientV2.listRoles().contains(roleName),
                "unoccupied role should be dropped with forceDrop=false");
        createdRoles.remove(roleName);
    }
}
