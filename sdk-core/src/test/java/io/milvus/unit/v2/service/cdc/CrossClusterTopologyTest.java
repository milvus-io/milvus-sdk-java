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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CrossClusterTopologyTest {
    @Test
    void builderBuildsAllFields() {
        CrossClusterTopology topology = CrossClusterTopology.builder()
                .sourceClusterId("src")
                .targetClusterId("tgt")
                .build();

        assertEquals("src", topology.getSourceClusterId());
        assertEquals("tgt", topology.getTargetClusterId());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        CrossClusterTopology topology = CrossClusterTopology.builder().build();

        assertNull(topology.getSourceClusterId());
        assertNull(topology.getTargetClusterId());
    }

    @Test
    void settersUpdateFields() {
        CrossClusterTopology topology = CrossClusterTopology.builder().build();

        topology.setSourceClusterId("src2");
        topology.setTargetClusterId("tgt2");

        assertEquals("src2", topology.getSourceClusterId());
        assertEquals("tgt2", topology.getTargetClusterId());
    }

    @Test
    void toGRPCAndFromGRPCCovertsAllFields() {
        io.milvus.grpc.CrossClusterTopology grpc = io.milvus.grpc.CrossClusterTopology.newBuilder()
                .setSourceClusterId("src")
                .setTargetClusterId("tgt")
                .build();

        CrossClusterTopology topology = CrossClusterTopology.fromGRPC(grpc);

        assertEquals("src", topology.getSourceClusterId());
        assertEquals("tgt", topology.getTargetClusterId());

        io.milvus.grpc.CrossClusterTopology roundTrip = topology.toGRPC();
        assertEquals("src", roundTrip.getSourceClusterId());
        assertEquals("tgt", roundTrip.getTargetClusterId());
    }

    @Test
    void toStringContainsFields() {
        CrossClusterTopology topology = CrossClusterTopology.builder()
                .sourceClusterId("src")
                .targetClusterId("tgt")
                .build();

        String text = topology.toString();
        assertEquals("CrossClusterTopology{sourceClusterId='src', targetClusterId='tgt'}", text);
    }
}
