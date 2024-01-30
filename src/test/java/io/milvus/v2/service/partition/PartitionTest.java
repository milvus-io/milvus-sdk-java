package io.milvus.v2.service.partition;

import io.milvus.v2.BaseTest;
import io.milvus.v2.service.partition.request.*;
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
        client_v2.createPartition(req);
    }

    @Test
    void testDropPartition() {
        DropPartitionReq req = DropPartitionReq.builder()
                .collectionName("test")
                .partitionName("test")
                .build();
        client_v2.dropPartition(req);
    }

    @Test
    void testHasPartition() {
        HasPartitionReq req = HasPartitionReq.builder()
                .collectionName("test")
                .partitionName("_default")
                .build();
        Boolean res = client_v2.hasPartition(req);
    }

    @Test
    void testListPartitions() {
        ListPartitionsReq req = ListPartitionsReq.builder()
                .collectionName("test")
                .build();
        List<String> res = client_v2.listPartitions(req);
        logger.info("resp: {}", res);
    }

    @Test
    void testLoadPartition() {
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("test");
        LoadPartitionsReq req = LoadPartitionsReq.builder()
                .collectionName("test")
                .partitionNames(partitionNames)
                .build();
        client_v2.loadPartitions(req);

    }

    @Test
    void testReleasePartition() {
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("test");

        ReleasePartitionsReq req = ReleasePartitionsReq.builder()
                .collectionName("test")
                .partitionNames(partitionNames)
                .build();
        client_v2.releasePartitions(req);

    }
}