package io.milvus.v2.service.rbac;

import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class UserTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UserTest.class);

    @Test
    void listUsers() {
        List<String> resp = client_v2.listUsers();
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeUser() {
        DescribeUserReq req = DescribeUserReq.builder()
                .userName("test")
                .build();
        DescribeUserResp resp = client_v2.describeUser(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testCreateUser() {
        CreateUserReq req = CreateUserReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .build();
        client_v2.createUser(req);

    }

    @Test
    void testUpdatePassword() {
        UpdatePasswordReq req = UpdatePasswordReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .newPassword("Zilliz@2024")
                .build();
        client_v2.updatePassword(req);
    }

    @Test
    void testDropUser() {
        DropUserReq req = DropUserReq.builder()
                .userName("test")
                .build();
        client_v2.dropUser(req);

    }
}