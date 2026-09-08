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

import io.milvus.v2.service.collection.ShardReplica;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ShardReplicaTest {

    @Test
    void builderBuildsWithAllFields() {
        List<Long> nodeIds = Arrays.asList(10L, 11L);
        ShardReplica replica = ShardReplica.builder()
                .leaderID(10L)
                .leaderAddress("host:19530")
                .channelName("by-dev-rootcoord-dml_0")
                .nodeIDs(nodeIds)
                .build();

        assertEquals(10L, replica.getLeaderID());
        assertEquals("host:19530", replica.getLeaderAddress());
        assertEquals("by-dev-rootcoord-dml_0", replica.getChannelName());
        assertEquals(nodeIds, replica.getNodeIDs());
    }

    @Test
    void defaultsApply() {
        ShardReplica replica = ShardReplica.builder().build();
        assertEquals("", replica.getChannelName());
        assertTrue(replica.getNodeIDs().isEmpty());
    }

    @Test
    void settersUpdateGetters() {
        ShardReplica replica = ShardReplica.builder().build();

        replica.setLeaderID(1L);
        assertEquals(1L, replica.getLeaderID());

        replica.setLeaderAddress("a:19530");
        assertEquals("a:19530", replica.getLeaderAddress());

        replica.setChannelName("ch");
        assertEquals("ch", replica.getChannelName());

        List<Long> nodeIds = Arrays.asList(2L, 3L);
        replica.setNodeIDs(nodeIds);
        assertEquals(nodeIds, replica.getNodeIDs());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(ShardReplica.builder());
    }

    @Test
    void toStringContainsFields() {
        ShardReplica replica = ShardReplica.builder().leaderID(1L).build();
        assertTrue(replica.toString().contains("1"));
    }
}
