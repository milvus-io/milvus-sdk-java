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

package io.milvus.v2.client.globalcluster;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class GlobalClusterTest {

    // ==================== isGlobalEndpoint tests ====================

    @Test
    public void testIsGlobalEndpoint_withGlobalClusterInUri() {
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://my-instance.global-cluster.cloud.zilliz.com:443"));
    }

    @Test
    public void testIsGlobalEndpoint_caseInsensitive() {
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://my-instance.GLOBAL-CLUSTER.cloud.zilliz.com:443"));
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://my-instance.Global-Cluster.cloud.zilliz.com:443"));
    }

    @Test
    public void testIsGlobalEndpoint_regularEndpoint() {
        assertFalse(GlobalClusterUtils.isGlobalEndpoint("https://my-instance.cloud.zilliz.com:443"));
        assertFalse(GlobalClusterUtils.isGlobalEndpoint("http://localhost:19530"));
    }

    @Test
    public void testIsGlobalEndpoint_nullUri() {
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(null));
    }

    @Test
    public void testIsGlobalEndpoint_emptyUri() {
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(""));
    }

    // ==================== buildTopologyUrl tests ====================

    @Test
    public void testBuildTopologyUrl_httpsUri() {
        String url = GlobalClusterUtils.buildTopologyUrl("https://my.global-cluster.cloud.zilliz.com:443");
        assertEquals("https://my.global-cluster.cloud.zilliz.com:443/global-cluster/topology", url);
    }

    @Test
    public void testBuildTopologyUrl_httpUpgradedToHttps() {
        String url = GlobalClusterUtils.buildTopologyUrl("http://my.global-cluster.cloud.zilliz.com");
        assertEquals("https://my.global-cluster.cloud.zilliz.com/global-cluster/topology", url);
    }

    @Test
    public void testBuildTopologyUrl_noScheme() {
        String url = GlobalClusterUtils.buildTopologyUrl("my.global-cluster.cloud.zilliz.com");
        assertEquals("https://my.global-cluster.cloud.zilliz.com/global-cluster/topology", url);
    }

    @Test
    public void testBuildTopologyUrl_trailingSlash() {
        String url = GlobalClusterUtils.buildTopologyUrl("https://my.global-cluster.cloud.zilliz.com/");
        assertEquals("https://my.global-cluster.cloud.zilliz.com/global-cluster/topology", url);
    }

    @Test
    public void testBuildTopologyUrl_withWhitespace() {
        String url = GlobalClusterUtils.buildTopologyUrl("  https://my.global-cluster.cloud.zilliz.com  ");
        assertEquals("https://my.global-cluster.cloud.zilliz.com/global-cluster/topology", url);
    }

    // ==================== parseTopologyResponse tests ====================

    @Test
    public void testParseTopologyResponse_validResponse() {
        String json = "{\"code\": 0, \"data\": {\"version\": 1, \"clusters\": [" +
                "{\"clusterId\": \"cluster-1\", \"endpoint\": \"c1.cloud.zilliz.com:443\", \"capability\": 3}," +
                "{\"clusterId\": \"cluster-2\", \"endpoint\": \"c2.cloud.zilliz.com:443\", \"capability\": 1}" +
                "]}}";

        GlobalTopology topology = GlobalClusterUtils.parseTopologyResponse(json);

        assertEquals(1L, topology.getVersion());
        assertEquals(2, topology.getClusters().size());

        ClusterInfo primary = topology.getClusters().get(0);
        assertEquals("cluster-1", primary.getClusterId());
        assertEquals("c1.cloud.zilliz.com:443", primary.getEndpoint());
        assertEquals(3, primary.getCapability());
        assertTrue(primary.isPrimary());

        ClusterInfo secondary = topology.getClusters().get(1);
        assertEquals("cluster-2", secondary.getClusterId());
        assertEquals("c2.cloud.zilliz.com:443", secondary.getEndpoint());
        assertEquals(1, secondary.getCapability());
        assertFalse(secondary.isPrimary());
    }

    @Test
    public void testParseTopologyResponse_errorCode() {
        String json = "{\"code\": 1, \"message\": \"unauthorized\"}";

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                GlobalClusterUtils.parseTopologyResponse(json));
        assertTrue(ex.getMessage().contains("unauthorized"));
    }

    @Test
    public void testParseTopologyResponse_errorCodeNoMessage() {
        String json = "{\"code\": 2}";

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                GlobalClusterUtils.parseTopologyResponse(json));
        assertTrue(ex.getMessage().contains("unknown error"));
    }

    @Test
    public void testParseTopologyResponse_multipleClusters() {
        String json = "{\"code\": 0, \"data\": {\"version\": 5, \"clusters\": [" +
                "{\"clusterId\": \"c1\", \"endpoint\": \"e1:443\", \"capability\": 1}," +
                "{\"clusterId\": \"c2\", \"endpoint\": \"e2:443\", \"capability\": 3}," +
                "{\"clusterId\": \"c3\", \"endpoint\": \"e3:443\", \"capability\": 1}" +
                "]}}";

        GlobalTopology topology = GlobalClusterUtils.parseTopologyResponse(json);
        assertEquals(5L, topology.getVersion());
        assertEquals(3, topology.getClusters().size());

        // getPrimary should return the first writable cluster
        ClusterInfo primary = topology.getPrimary();
        assertEquals("c2", primary.getClusterId());
        assertEquals("e2:443", primary.getEndpoint());
    }

    // ==================== ClusterCapability tests ====================

    @Test
    public void testClusterCapabilityValues() {
        assertEquals(0b01, ClusterCapability.READABLE);
        assertEquals(0b10, ClusterCapability.WRITABLE);
        assertEquals(0b11, ClusterCapability.PRIMARY);
        assertEquals(ClusterCapability.READABLE | ClusterCapability.WRITABLE, ClusterCapability.PRIMARY);
    }

    // ==================== ClusterInfo tests ====================

    @Test
    public void testClusterInfo_isPrimary_writable() {
        ClusterInfo info = new ClusterInfo("id1", "endpoint1", ClusterCapability.WRITABLE);
        assertTrue(info.isPrimary());
    }

    @Test
    public void testClusterInfo_isPrimary_primary() {
        ClusterInfo info = new ClusterInfo("id1", "endpoint1", ClusterCapability.PRIMARY);
        assertTrue(info.isPrimary());
    }

    @Test
    public void testClusterInfo_isPrimary_readableOnly() {
        ClusterInfo info = new ClusterInfo("id1", "endpoint1", ClusterCapability.READABLE);
        assertFalse(info.isPrimary());
    }

    @Test
    public void testClusterInfo_isPrimary_zero() {
        ClusterInfo info = new ClusterInfo("id1", "endpoint1", 0);
        assertFalse(info.isPrimary());
    }

    @Test
    public void testClusterInfo_getters() {
        ClusterInfo info = new ClusterInfo("cluster-abc", "host.com:443", 3);
        assertEquals("cluster-abc", info.getClusterId());
        assertEquals("host.com:443", info.getEndpoint());
        assertEquals(3, info.getCapability());
    }

    @Test
    public void testClusterInfo_toString() {
        ClusterInfo info = new ClusterInfo("c1", "e1", 3);
        String str = info.toString();
        assertTrue(str.contains("c1"));
        assertTrue(str.contains("e1"));
        assertTrue(str.contains("3"));
    }

    // ==================== GlobalTopology tests ====================

    @Test
    public void testGlobalTopology_getPrimary_found() {
        List<ClusterInfo> clusters = Arrays.asList(
                new ClusterInfo("c1", "e1:443", ClusterCapability.READABLE),
                new ClusterInfo("c2", "e2:443", ClusterCapability.PRIMARY)
        );
        GlobalTopology topology = new GlobalTopology(1, clusters);

        ClusterInfo primary = topology.getPrimary();
        assertEquals("c2", primary.getClusterId());
        assertEquals("e2:443", primary.getEndpoint());
    }

    @Test
    public void testGlobalTopology_getPrimary_firstWritable() {
        List<ClusterInfo> clusters = Arrays.asList(
                new ClusterInfo("c1", "e1:443", ClusterCapability.WRITABLE),
                new ClusterInfo("c2", "e2:443", ClusterCapability.PRIMARY)
        );
        GlobalTopology topology = new GlobalTopology(1, clusters);

        // Should return the first one with WRITABLE bit set
        ClusterInfo primary = topology.getPrimary();
        assertEquals("c1", primary.getClusterId());
    }

    @Test
    public void testGlobalTopology_getPrimary_noneFound() {
        List<ClusterInfo> clusters = Arrays.asList(
                new ClusterInfo("c1", "e1:443", ClusterCapability.READABLE),
                new ClusterInfo("c2", "e2:443", ClusterCapability.READABLE)
        );
        GlobalTopology topology = new GlobalTopology(1, clusters);

        assertThrows(IllegalStateException.class, topology::getPrimary);
    }

    @Test
    public void testGlobalTopology_getPrimary_emptyClusters() {
        GlobalTopology topology = new GlobalTopology(1, Collections.<ClusterInfo>emptyList());
        assertThrows(IllegalStateException.class, topology::getPrimary);
    }

    @Test
    public void testGlobalTopology_getters() {
        List<ClusterInfo> clusters = Arrays.asList(
                new ClusterInfo("c1", "e1", 3)
        );
        GlobalTopology topology = new GlobalTopology(42, clusters);
        assertEquals(42L, topology.getVersion());
        assertEquals(1, topology.getClusters().size());
    }

    @Test
    public void testGlobalTopology_largeVersion() {
        String json = "{\"code\": 0, \"data\": {\"version\": \"2029391083243028480\", \"clusters\": [" +
                "{\"clusterId\": \"c1\", \"endpoint\": \"e1:443\", \"capability\": 3}" +
                "]}}";
        GlobalTopology topology = GlobalClusterUtils.parseTopologyResponse(json);
        assertEquals(2029391083243028480L, topology.getVersion());
    }

    @Test
    public void testGlobalTopology_toString() {
        List<ClusterInfo> clusters = Arrays.asList(
                new ClusterInfo("c1", "e1", 3)
        );
        GlobalTopology topology = new GlobalTopology(7, clusters);
        String str = topology.toString();
        assertTrue(str.contains("7"));
        assertTrue(str.contains("c1"));
    }

    // ==================== TopologyRefresher tests ====================

    @Test
    public void testTopologyRefresher_stopShutdownsExecutor() {
        TopologyRefresher refresher = new TopologyRefresher(
                "https://test.global-cluster.com", "token", 1,
                topology -> {});
        refresher.start();
        refresher.stop();
        // Should not throw; executor is shut down
    }

    @Test
    public void testTopologyRefresher_triggerRefreshAfterStop() {
        TopologyRefresher refresher = new TopologyRefresher(
                "https://test.global-cluster.com", "token", 1,
                topology -> {});
        refresher.start();
        refresher.stop();
        // triggerRefresh after stop should not throw (executor rejects silently)
        try {
            refresher.triggerRefresh();
        } catch (Exception e) {
            // RejectedExecutionException is acceptable after shutdown
        }
    }

    // ==================== RpcUtils global refresh trigger tests ====================

    @Test
    public void testRpcUtils_globalRefreshTrigger() {
        io.milvus.v2.utils.RpcUtils rpcUtils = new io.milvus.v2.utils.RpcUtils();

        AtomicReference<Boolean> triggered = new AtomicReference<>(false);
        rpcUtils.setGlobalRefreshTrigger(() -> triggered.set(true));

        // The trigger is set but only called internally on UNAVAILABLE errors.
        // Just verify it can be set without error.
        assertFalse(triggered.get());
    }

    // ==================== End-to-end parse + getPrimary test ====================

    @Test
    public void testEndToEnd_parseAndGetPrimary() {
        // Simulate a real API response
        String json = "{\"code\": 0, \"data\": {\"version\": 3, \"clusters\": [" +
                "{\"clusterId\": \"us-east-1\", \"endpoint\": \"us-east.milvus.cloud.zilliz.com:443\", \"capability\": 1}," +
                "{\"clusterId\": \"eu-west-1\", \"endpoint\": \"eu-west.milvus.cloud.zilliz.com:443\", \"capability\": 3}," +
                "{\"clusterId\": \"ap-south-1\", \"endpoint\": \"ap-south.milvus.cloud.zilliz.com:443\", \"capability\": 1}" +
                "]}}";

        GlobalTopology topology = GlobalClusterUtils.parseTopologyResponse(json);
        assertEquals(3L, topology.getVersion());
        assertEquals(3, topology.getClusters().size());

        ClusterInfo primary = topology.getPrimary();
        assertEquals("eu-west-1", primary.getClusterId());
        assertEquals("eu-west.milvus.cloud.zilliz.com:443", primary.getEndpoint());
        assertTrue(primary.isPrimary());
        assertEquals(ClusterCapability.PRIMARY, primary.getCapability());
    }

    @Test
    public void testEndToEnd_versionChangeDetection() {
        // Simulate two topology responses with different versions
        String jsonV1 = "{\"code\": 0, \"data\": {\"version\": 1, \"clusters\": [" +
                "{\"clusterId\": \"c1\", \"endpoint\": \"e1:443\", \"capability\": 3}" +
                "]}}";
        String jsonV2 = "{\"code\": 0, \"data\": {\"version\": 2, \"clusters\": [" +
                "{\"clusterId\": \"c2\", \"endpoint\": \"e2:443\", \"capability\": 3}" +
                "]}}";

        GlobalTopology t1 = GlobalClusterUtils.parseTopologyResponse(jsonV1);
        GlobalTopology t2 = GlobalClusterUtils.parseTopologyResponse(jsonV2);

        assertEquals(1L, t1.getVersion());
        assertEquals(2L, t2.getVersion());
        assertNotEquals(t1.getVersion(), t2.getVersion());

        // Primary changed
        assertEquals("e1:443", t1.getPrimary().getEndpoint());
        assertEquals("e2:443", t2.getPrimary().getEndpoint());
    }
}
