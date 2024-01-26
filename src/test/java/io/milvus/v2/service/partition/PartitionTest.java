package io.milvus.v2.service.partition;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.partition.request.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class PartitionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(PartitionTest.class);

    @Test
    void testCreatePartition() {
        CreatePartitionReq req = CreatePartitionReq.builder()
                .collectionName("test")
                .partitionName("test")
                .build();
        R<RpcStatus> res = client_v2.createPartition(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }

    @Test
    void testDropPartition() {
        DropPartitionReq req = DropPartitionReq.builder()
                .collectionName("test")
                .partitionName("test")
                .build();
        R<RpcStatus> res = client_v2.dropPartition(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }

    @Test
    void testHasPartition() {
        HasPartitionReq req = HasPartitionReq.builder()
                .collectionName("test")
                .partitionName("_default")
                .build();
        R<Boolean> res = client_v2.hasPartition(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }

    @Test
    void testListPartitions() {
        ListPartitionsReq req = ListPartitionsReq.builder()
                .collectionName("test")
                .build();
        R<List<String>> res = client_v2.listPartitions(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }

    @Test
    void testLoadPartition() {
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("test");
        LoadPartitionsReq req = LoadPartitionsReq.builder()
                .collectionName("test")
                .partitionNames(partitionNames)
                .build();
        R<RpcStatus> res = client_v2.loadPartitions(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }

    @Test
    void testReleasePartition() {
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("test");

        ReleasePartitionsReq req = ReleasePartitionsReq.builder()
                .collectionName("test")
                .partitionNames(partitionNames)
                .build();
        R<RpcStatus> res = client_v2.releasePartitions(req);
        logger.info("resp: {}", res);
        Assertions.assertEquals(0, res.getStatus());
    }
}