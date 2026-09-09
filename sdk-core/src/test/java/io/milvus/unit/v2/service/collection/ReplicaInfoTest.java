/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.service.collection.ReplicaInfo;
import io.milvus.v2.service.collection.ShardReplica;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ReplicaInfoTest {

    @Test
    void builderBuildsWithAllFields() {
        List<Long> partitionIds = Arrays.asList(1L, 2L);
        ShardReplica shardReplica = ShardReplica.builder()
                .leaderID(10L)
                .leaderAddress("host:19530")
                .channelName("by-dev-rootcoord-dml_0")
                .nodeIDs(Arrays.asList(10L, 11L))
                .build();
        List<Long> nodeIds = Arrays.asList(10L, 11L, 12L);
        Map<String, Integer> numOutbound = Collections.singletonMap("rg", 2);

        ReplicaInfo info = ReplicaInfo.builder()
                .replicaID(100L)
                .collectionID(200L)
                .partitionIDs(partitionIds)
                .shardReplicas(Collections.singletonList(shardReplica))
                .nodeIDs(nodeIds)
                .resourceGroupName("__default_resource_group")
                .numOutboundNode(numOutbound)
                .build();

        assertEquals(100L, info.getReplicaID());
        assertEquals(200L, info.getCollectionID());
        assertEquals(partitionIds, info.getPartitionIDs());
        assertEquals(Collections.singletonList(shardReplica), info.getShardReplicas());
        assertEquals(nodeIds, info.getNodeIDs());
        assertEquals("__default_resource_group", info.getResourceGroupName());
        assertEquals(numOutbound, info.getNumOutboundNode());
    }

    @Test
    void defaultsApply() {
        ReplicaInfo info = ReplicaInfo.builder().build();
        assertTrue(info.getPartitionIDs().isEmpty());
        assertTrue(info.getShardReplicas().isEmpty());
        assertTrue(info.getNodeIDs().isEmpty());
        assertEquals("", info.getResourceGroupName());
        assertTrue(info.getNumOutboundNode().isEmpty());
    }

    @Test
    void settersUpdateGetters() {
        ReplicaInfo info = ReplicaInfo.builder().build();

        info.setReplicaID(1L);
        assertEquals(1L, info.getReplicaID());

        info.setCollectionID(2L);
        assertEquals(2L, info.getCollectionID());

        List<Long> partitionIds = Arrays.asList(3L, 4L);
        info.setPartitionIDs(partitionIds);
        assertEquals(partitionIds, info.getPartitionIDs());

        ShardReplica shardReplica = ShardReplica.builder().leaderID(5L).build();
        info.setShardReplicas(Collections.singletonList(shardReplica));
        assertEquals(Collections.singletonList(shardReplica), info.getShardReplicas());

        List<Long> nodeIds = Arrays.asList(6L, 7L);
        info.setNodeIDs(nodeIds);
        assertEquals(nodeIds, info.getNodeIDs());

        info.setResourceGroupName("rg");
        assertEquals("rg", info.getResourceGroupName());

        Map<String, Integer> numOutbound = Collections.singletonMap("rg", 1);
        info.setNumOutboundNode(numOutbound);
        assertEquals(numOutbound, info.getNumOutboundNode());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(ReplicaInfo.builder());
    }

    @Test
    void toStringContainsFields() {
        ReplicaInfo info = ReplicaInfo.builder().replicaID(100L).build();
        assertTrue(info.toString().contains("100"));
    }
}
