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

package io.milvus.v2.service.cdc;

import io.milvus.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CDCServiceTest extends BaseTest {
    @Test
    void testGetReplicateInfo() {
        GetReplicateInfoResp resp = client_v2.getReplicateInfo(GetReplicateInfoReq.builder()
                .sourceClusterId("source_cluster")
                .targetPchannel("by-dev-rootcoord-dml_0")
                .build());

        GetReplicateInfoResp.ReplicateCheckpoint checkpoint = resp.getCheckpoint();
        assertNotNull(checkpoint);
        assertEquals("source_cluster", checkpoint.getClusterId());
        assertEquals("by-dev-rootcoord-dml_0", checkpoint.getPchannel());
        assertEquals(1000L, checkpoint.getTimeTick());
        assertNotNull(checkpoint.getMessageID());
        assertEquals("message-id-1", checkpoint.getMessageID().getId());
        assertEquals("RocksMQ", checkpoint.getMessageID().getWalName());

        GetReplicateInfoResp.ReplicateCheckpoint salvageCheckpoint = resp.getSalvageCheckpoint();
        assertNotNull(salvageCheckpoint);
        assertEquals("source_cluster", salvageCheckpoint.getClusterId());
        assertEquals("by-dev-rootcoord-dml_0", salvageCheckpoint.getPchannel());
        assertEquals(2000L, salvageCheckpoint.getTimeTick());
        assertNotNull(salvageCheckpoint.getMessageID());
        assertEquals("message-id-2", salvageCheckpoint.getMessageID().getId());
        assertEquals("Kafka", salvageCheckpoint.getMessageID().getWalName());
    }

    @Test
    void testGetReplicateInfoValidatesSourceClusterId() {
        MilvusClientException exception = assertThrows(MilvusClientException.class, () -> client_v2.getReplicateInfo(GetReplicateInfoReq.builder()
                .sourceClusterId("")
                .targetPchannel("by-dev-rootcoord-dml_0")
                .build()));
        assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testGetReplicateInfoValidatesTargetPchannel() {
        MilvusClientException exception = assertThrows(MilvusClientException.class, () -> client_v2.getReplicateInfo(GetReplicateInfoReq.builder()
                .sourceClusterId("source_cluster")
                .targetPchannel("")
                .build()));
        assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testGetReplicateConfiguration() {
        GetReplicateConfigurationResp resp = client_v2.getReplicateConfiguration();

        ReplicateConfiguration replicateConfiguration = resp.getReplicateConfiguration();
        assertNotNull(replicateConfiguration);
        assertEquals(2, replicateConfiguration.getClusters().size());
        assertEquals("source_cluster", replicateConfiguration.getClusters().get(0).getClusterId());
        assertEquals("http://source.example.com:19530", replicateConfiguration.getClusters().get(0).getUri());
        assertEquals("source-token", replicateConfiguration.getClusters().get(0).getToken());
        assertEquals("by-dev-rootcoord-dml_0", replicateConfiguration.getClusters().get(0).getPchannels().get(0));
        assertEquals(1, replicateConfiguration.getCrossClusterTopologies().size());
        assertEquals("source_cluster", replicateConfiguration.getCrossClusterTopologies().get(0).getSourceClusterId());
        assertEquals("target_cluster", replicateConfiguration.getCrossClusterTopologies().get(0).getTargetClusterId());
    }
}
