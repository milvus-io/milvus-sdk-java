/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.cdc;

import io.milvus.v2.service.cdc.request.MilvusCluster;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class MilvusClusterTest {
    @Test
    void builderBuildsAllFields() {
        List<String> pchannels = List.of("chan-1", "chan-2");

        MilvusCluster cluster = MilvusCluster.builder()
                .clusterId("cluster-1")
                .uri("http://localhost:19530")
                .token("token")
                .pchannels(pchannels)
                .build();

        assertEquals("cluster-1", cluster.getClusterId());
        assertEquals("http://localhost:19530", cluster.getUri());
        assertEquals("token", cluster.getToken());
        assertSame(pchannels, cluster.getPchannels());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        MilvusCluster cluster = MilvusCluster.builder().build();

        assertNull(cluster.getClusterId());
        assertNull(cluster.getUri());
        assertNull(cluster.getToken());
        assertNull(cluster.getPchannels());
    }

    @Test
    void settersUpdateFields() {
        MilvusCluster cluster = MilvusCluster.builder().build();

        cluster.setClusterId("cluster-2");
        cluster.setUri("http://example.com");
        cluster.setToken("t2");
        cluster.setPchannels(List.of("c"));

        assertEquals("cluster-2", cluster.getClusterId());
        assertEquals("http://example.com", cluster.getUri());
        assertEquals("t2", cluster.getToken());
        assertEquals(List.of("c"), cluster.getPchannels());
    }

    @Test
    void toGRPCAndFromGRPCCovertsAllFields() {
        io.milvus.grpc.ConnectionParam connectionParam = io.milvus.grpc.ConnectionParam.newBuilder()
                .setUri("http://localhost:19530")
                .setToken("secret")
                .build();
        io.milvus.grpc.MilvusCluster grpc = io.milvus.grpc.MilvusCluster.newBuilder()
                .setClusterId("cluster-1")
                .setConnectionParam(connectionParam)
                .addAllPchannels(List.of("chan-1"))
                .build();

        MilvusCluster cluster = MilvusCluster.fromGRPC(grpc);

        assertEquals("cluster-1", cluster.getClusterId());
        assertEquals("http://localhost:19530", cluster.getUri());
        assertEquals("secret", cluster.getToken());
        assertEquals(List.of("chan-1"), cluster.getPchannels());

        io.milvus.grpc.MilvusCluster roundTrip = cluster.toGRPC();
        assertEquals("cluster-1", roundTrip.getClusterId());
        assertEquals("http://localhost:19530", roundTrip.getConnectionParam().getUri());
        assertEquals("secret", roundTrip.getConnectionParam().getToken());
        assertEquals(List.of("chan-1"), roundTrip.getPchannelsList());
    }

    @Test
    void toGRPCWithoutTokenAndPchannels() {
        MilvusCluster cluster = MilvusCluster.builder()
                .clusterId("cluster-1")
                .uri("http://localhost:19530")
                .build();

        io.milvus.grpc.MilvusCluster grpc = cluster.toGRPC();

        assertEquals("cluster-1", grpc.getClusterId());
        assertEquals("http://localhost:19530", grpc.getConnectionParam().getUri());
        assertEquals("", grpc.getConnectionParam().getToken());
        assertEquals(0, grpc.getPchannelsCount());
    }

    @Test
    void toStringRedactsCredentials() {
        MilvusCluster cluster = MilvusCluster.builder()
                .clusterId("cluster-1")
                .uri("http://user:pass@localhost:19530")
                .token("secret-token")
                .pchannels(List.of("c"))
                .build();

        String text = cluster.toString();

        assertTrue(text.contains("cluster-1"));
        assertFalse(text.contains("secret-token"));
        assertFalse(text.contains("user:pass"));
        assertTrue(text.contains("pchannels=[c]"));
    }
}
