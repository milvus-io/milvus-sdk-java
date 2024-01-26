package io.milvus.v2.service.utility;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.utility.request.AlterAliasReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class UtilityTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UtilityTest.class);

    @Test
    void testFlush() {
        FlushReq req = FlushReq.builder()
                .collectionName("test")
                .build();
        R<RpcStatus> statusR = client_v2.flush(req);
        logger.info("resp: {}", statusR.getData());
        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
    }

    @Test
    void testCreateAlias() {
        CreateAliasReq req = CreateAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        R<RpcStatus> statusR = client_v2.createAlias(req);
        logger.info("resp: {}", statusR.getData());
        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
    }
    @Test
    void testDropAlias() {
        DropAliasReq req = DropAliasReq.builder()
                .alias("test_alias")
                .build();
        R<RpcStatus> statusR = client_v2.dropAlias(req);
        logger.info("resp: {}", statusR.getData());
        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
    }
    @Test
    void testAlterAlias() {
        AlterAliasReq req = AlterAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        R<RpcStatus> statusR = client_v2.alterAlias(req);
        logger.info("resp: {}", statusR.getData());
        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
    }
//    @Test
//    void describeAlias() {
//        R<DescribeAliasResp> statusR = clientv_2.describeAlias("test_alias");
//        logger.info("resp: {}", statusR.getData());
//        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
//    }
//    @Test
//    void listAliases() {
//        R<ListAliasResp> statusR = clientv_2.listAliases();
//        logger.info("resp: {}", statusR.getData());
//        assertEquals(R.Status.Success.getCode(), statusR.getStatus());
//    }
}