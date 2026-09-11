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

import io.milvus.v2.service.cdc.request.CrossClusterTopology;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class ReplicateConfigurationTest {
    @Test
    void builderBuildsAllFields() {
        List<MilvusCluster> clusters = Arrays.asList(MilvusCluster.builder().clusterId("c1").build());
        List<CrossClusterTopology> topologies = Arrays.asList(
                CrossClusterTopology.builder().sourceClusterId("c1").targetClusterId("c2").build());

        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(clusters)
                .crossClusterTopologies(topologies)
                .build();

        assertSame(clusters, configuration.getClusters());
        assertSame(topologies, configuration.getCrossClusterTopologies());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();

        assertNull(configuration.getClusters());
        assertNull(configuration.getCrossClusterTopologies());
    }

    @Test
    void settersUpdateFields() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();

        configuration.setClusters(Arrays.asList(MilvusCluster.builder().clusterId("c1").build()));
        configuration.setCrossClusterTopologies(Arrays.asList(
                CrossClusterTopology.builder().sourceClusterId("c1").targetClusterId("c2").build()));

        assertEquals(1, configuration.getClusters().size());
        assertEquals(1, configuration.getCrossClusterTopologies().size());
    }

    @Test
    void toGRPCAndFromGRPCCovertsAllFields() {
        io.milvus.grpc.ReplicateConfiguration grpc = io.milvus.grpc.ReplicateConfiguration.newBuilder()
                .addClusters(io.milvus.grpc.MilvusCluster.newBuilder()
                        .setClusterId("c1")
                        .setConnectionParam(io.milvus.grpc.ConnectionParam.newBuilder()
                                .setUri("http://localhost:19530")
                                .setToken("token")
                                .build())
                        .addAllPchannels(Arrays.asList("chan-1")))
                .addCrossClusterTopology(io.milvus.grpc.CrossClusterTopology.newBuilder()
                        .setSourceClusterId("c1")
                        .setTargetClusterId("c2"))
                .build();

        ReplicateConfiguration configuration = ReplicateConfiguration.fromGRPC(grpc);

        assertEquals(1, configuration.getClusters().size());
        assertEquals("c1", configuration.getClusters().get(0).getClusterId());
        assertEquals(1, configuration.getCrossClusterTopologies().size());
        assertEquals("c1", configuration.getCrossClusterTopologies().get(0).getSourceClusterId());

        io.milvus.grpc.ReplicateConfiguration roundTrip = configuration.toGRPC();
        assertEquals(1, roundTrip.getClustersCount());
        assertEquals(1, roundTrip.getCrossClusterTopologyCount());
    }

    @Test
    void toGRPCWithNullCollectionsProducesEmptyMessage() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();

        io.milvus.grpc.ReplicateConfiguration grpc = configuration.toGRPC();

        assertEquals(0, grpc.getClustersCount());
        assertEquals(0, grpc.getCrossClusterTopologyCount());
    }

    @Test
    void toStringContainsFields() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(Arrays.asList(MilvusCluster.builder().clusterId("c1").build()))
                .build();

        String text = configuration.toString();
        assertEquals("ReplicateConfiguration{clusters=[MilvusCluster{clusterId='c1', uri='null', token='null', pchannels=null}], crossClusterTopologies=null}", text);
    }
}
