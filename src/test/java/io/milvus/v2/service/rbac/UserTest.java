package io.milvus.v2.service.rbac;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class UserTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UserTest.class);

    @Test
    void listUsers() {
        R<List<String>> resp = client_v2.listUsers();
        logger.info("resp: {}", resp);
        Assertions.assertEquals(resp.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDescribeUser() {
        DescribeUserReq req = DescribeUserReq.builder()
                .userName("test")
                .build();
        R<DescribeUserResp> resp = client_v2.describeUser(req);
        logger.info("resp: {}", resp);
        Assertions.assertEquals(resp.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testCreateUser() {
        CreateUserReq req = CreateUserReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .build();
        R<RpcStatus> resp = client_v2.createUser(req);
        logger.info("resp: {}", resp);
        Assertions.assertEquals(resp.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testUpdatePassword() {
        UpdatePasswordReq req = UpdatePasswordReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .newPassword("Zilliz@2024")
                .build();
        R<RpcStatus> resp = client_v2.updatePassword(req);
        logger.info("resp: {}", resp);
        Assertions.assertEquals(resp.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDropUser() {
        DropUserReq req = DropUserReq.builder()
                .userName("test")
                .build();
        R<RpcStatus> resp = client_v2.dropUser(req);
        logger.info("resp: {}", resp);
        Assertions.assertEquals(resp.getStatus(), R.Status.Success.getCode());
    }
}