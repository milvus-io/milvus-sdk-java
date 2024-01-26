package io.milvus.v2.service.rbac;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class RoleTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(RoleTest.class);

    @Test
    void testListRoles() {
        R<List<String>> roles = client_v2.listRoles();
        logger.info(roles.toString());
        Assertions.assertEquals(roles.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testCreateRole() {
        CreateRoleReq request = CreateRoleReq.builder()
                .roleName("test")
                .build();
        R<RpcStatus> statusR = client_v2.createRole(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDescribeRole() {
        DescribeRoleReq describeRoleReq = DescribeRoleReq.builder()
                .roleName("db_rw")
                .build();
        R<DescribeRoleResp> statusR = client_v2.describeRole(describeRoleReq);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDropRole() {
        DropRoleReq request = DropRoleReq.builder()
                .roleName("test")
                .build();
        R<RpcStatus> statusR = client_v2.dropRole(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testGrantPrivilege() {
        GrantPrivilegeReq request = GrantPrivilegeReq.builder()
                .roleName("db_rw")
                .objectName("")
                .objectType("")
                .privilege("")
                .build();
        R<RpcStatus> statusR = client_v2.grantPrivilege(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testRevokePrivilege() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder()
                .roleName("db_rw")
                .objectName("")
                .objectType("")
                .privilege("")
                .build();
        R<RpcStatus> statusR = client_v2.revokePrivilege(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testGrantRole() {
        GrantRoleReq request = GrantRoleReq.builder()
                .roleName("db_ro")
                .userName("test")
                .build();
        R<RpcStatus> statusR = client_v2.grantRole(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testRevokeRole() {
        RevokeRoleReq request = RevokeRoleReq.builder()
                .roleName("db_ro")
                .userName("test")
                .build();
        R<RpcStatus> statusR = client_v2.revokeRole(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }
}