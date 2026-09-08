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

package io.milvus.unit.v2.service.resourcegroup;

import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeResourceGroupRespTest {
    @Test
    void builderBuildsAllFields() {
        Map<String, Integer> loadedReplica = new HashMap<>();
        loadedReplica.put("coll", 2);
        Map<String, Integer> outgoingNode = new HashMap<>();
        outgoingNode.put("tgt", 1);
        Map<String, Integer> incomingNode = new HashMap<>();
        incomingNode.put("src", 1);
        ResourceGroupConfig config = ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(1))
                .withLimits(new ResourceGroupLimit(2))
                .build();
        NodeInfo node = NodeInfo.builder().nodeId(1L).address("localhost").hostname("host").build();

        DescribeResourceGroupResp response = DescribeResourceGroupResp.builder()
                .groupName("rg")
                .capacity(4)
                .numberOfAvailableNode(3)
                .numberOfLoadedReplica(loadedReplica)
                .numberOfOutgoingNode(outgoingNode)
                .numberOfIncomingNode(incomingNode)
                .config(config)
                .nodes(Collections.singletonList(node))
                .build();

        assertEquals("rg", response.getGroupName());
        assertEquals(Integer.valueOf(4), response.getCapacity());
        assertEquals(Integer.valueOf(3), response.getNumberOfAvailableNode());
        assertEquals(loadedReplica, response.getNumberOfLoadedReplica());
        assertEquals(outgoingNode, response.getNumberOfOutgoingNode());
        assertEquals(incomingNode, response.getNumberOfIncomingNode());
        assertEquals(config, response.getConfig());
        assertEquals(Collections.singletonList(node), response.getNodes());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DescribeResourceGroupResp response = DescribeResourceGroupResp.builder().build();

        assertNull(response.getGroupName());
        assertNull(response.getCapacity());
        assertNull(response.getNumberOfAvailableNode());
        assertTrue(response.getNumberOfLoadedReplica().isEmpty());
        assertTrue(response.getNumberOfOutgoingNode().isEmpty());
        assertTrue(response.getNumberOfIncomingNode().isEmpty());
        assertNull(response.getConfig());
        assertTrue(response.getNodes().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        DescribeResourceGroupResp response = DescribeResourceGroupResp.builder().build();

        response.setGroupName("rg");
        response.setCapacity(8);
        response.setNumberOfAvailableNode(7);
        response.setNumberOfLoadedReplica(new HashMap<>());
        response.setNumberOfOutgoingNode(new HashMap<>());
        response.setNumberOfIncomingNode(new HashMap<>());
        response.setConfig(null);
        response.setNodes(Collections.emptyList());

        assertEquals("rg", response.getGroupName());
        assertEquals(Integer.valueOf(8), response.getCapacity());
        assertEquals(Integer.valueOf(7), response.getNumberOfAvailableNode());
        assertTrue(response.getNumberOfLoadedReplica().isEmpty());
        assertTrue(response.getNumberOfOutgoingNode().isEmpty());
        assertTrue(response.getNumberOfIncomingNode().isEmpty());
        assertNull(response.getConfig());
        assertTrue(response.getNodes().isEmpty());
    }

    @Test
    void boundaryIntegerFieldsAreAccepted() {
        DescribeResourceGroupResp response = DescribeResourceGroupResp.builder()
                .capacity(0)
                .numberOfAvailableNode(0)
                .build();

        assertEquals(Integer.valueOf(0), response.getCapacity());
        assertEquals(Integer.valueOf(0), response.getNumberOfAvailableNode());
    }

    @Test
    void toStringContainsFields() {
        DescribeResourceGroupResp response = DescribeResourceGroupResp.builder()
                .groupName("rg")
                .capacity(1)
                .build();

        assertEquals("DescribeResourceGroupResp{groupName='rg', capacity=1, numberOfAvailableNode=null, "
                        + "numberOfLoadedReplica={}, numberOfOutgoingNode={}, numberOfIncomingNode={}, "
                        + "config=null, nodes=[]}",
                response.toString());
    }
}
