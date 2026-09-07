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

import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp.MessageID;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp.ReplicateCheckpoint;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class GetReplicateInfoRespTest {
    @Test
    void builderBuildsAllFields() {
        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.builder().clusterId("c1").build();
        ReplicateCheckpoint salvageCheckpoint = ReplicateCheckpoint.builder().clusterId("c2").build();

        GetReplicateInfoResp response = GetReplicateInfoResp.builder()
                .checkpoint(checkpoint)
                .salvageCheckpoint(salvageCheckpoint)
                .build();

        assertSame(checkpoint, response.getCheckpoint());
        assertSame(salvageCheckpoint, response.getSalvageCheckpoint());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        GetReplicateInfoResp response = GetReplicateInfoResp.builder().build();

        assertNull(response.getCheckpoint());
        assertNull(response.getSalvageCheckpoint());
    }

    @Test
    void settersUpdateFields() {
        GetReplicateInfoResp response = GetReplicateInfoResp.builder().build();

        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.builder().clusterId("c1").build();
        ReplicateCheckpoint salvage = ReplicateCheckpoint.builder().clusterId("c2").build();
        response.setCheckpoint(checkpoint);
        response.setSalvageCheckpoint(salvage);

        assertSame(checkpoint, response.getCheckpoint());
        assertSame(salvage, response.getSalvageCheckpoint());
    }

    @Test
    void replicateCheckpointBuilderBuildsAllFields() {
        MessageID messageID = MessageID.builder().id("id-1").walName("RocksMQ").build();

        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.builder()
                .clusterId("c1")
                .pchannel("chan-1")
                .messageID(messageID)
                .timeTick(100L)
                .build();

        assertEquals("c1", checkpoint.getClusterId());
        assertEquals("chan-1", checkpoint.getPchannel());
        assertSame(messageID, checkpoint.getMessageID());
        assertEquals(Long.valueOf(100L), checkpoint.getTimeTick());
    }

    @Test
    void replicateCheckpointSettersUpdateFields() {
        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.builder().build();

        MessageID messageID = MessageID.builder().id("id-2").walName("Kafka").build();
        checkpoint.setClusterId("c2");
        checkpoint.setPchannel("chan-2");
        checkpoint.setMessageID(messageID);
        checkpoint.setTimeTick(200L);

        assertEquals("c2", checkpoint.getClusterId());
        assertEquals("chan-2", checkpoint.getPchannel());
        assertSame(messageID, checkpoint.getMessageID());
        assertEquals(Long.valueOf(200L), checkpoint.getTimeTick());
    }

    @Test
    void messageIDBuilderAndSetters() {
        MessageID messageID = MessageID.builder()
                .id("id-1")
                .walName("WoodPecker")
                .build();

        assertEquals("id-1", messageID.getId());
        assertEquals("WoodPecker", messageID.getWalName());

        messageID.setId("id-2");
        messageID.setWalName("RocksMQ");

        assertEquals("id-2", messageID.getId());
        assertEquals("RocksMQ", messageID.getWalName());
    }

    @Test
    void messageIDFromGRPCConvertsWalName() {
        io.milvus.grpc.MessageID grpc = io.milvus.grpc.MessageID.newBuilder()
                .setId("id-1")
                .setWALName(io.milvus.grpc.WALName.Kafka)
                .build();

        MessageID messageID = MessageID.fromGRPC(grpc);

        assertEquals("id-1", messageID.getId());
        assertEquals("Kafka", messageID.getWalName());
    }

    @Test
    void replicateCheckpointFromGRPCConvertsMessageId() {
        io.milvus.grpc.ReplicateCheckpoint grpc = io.milvus.grpc.ReplicateCheckpoint.newBuilder()
                .setClusterId("c1")
                .setPchannel("chan-1")
                .setMessageId(io.milvus.grpc.MessageID.newBuilder()
                        .setId("id-1")
                        .setWALName(io.milvus.grpc.WALName.RocksMQ)
                        .build())
                .setTimeTick(100L)
                .build();

        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.fromGRPC(grpc);

        assertEquals("c1", checkpoint.getClusterId());
        assertEquals("chan-1", checkpoint.getPchannel());
        assertEquals("id-1", checkpoint.getMessageID().getId());
        assertEquals(Long.valueOf(100L), checkpoint.getTimeTick());
    }

    @Test
    void replicateCheckpointFromGRPCWithoutMessageIdYieldsNull() {
        io.milvus.grpc.ReplicateCheckpoint grpc = io.milvus.grpc.ReplicateCheckpoint.newBuilder()
                .setClusterId("c1")
                .setPchannel("chan-1")
                .build();

        ReplicateCheckpoint checkpoint = ReplicateCheckpoint.fromGRPC(grpc);

        assertEquals("c1", checkpoint.getClusterId());
        assertNull(checkpoint.getMessageID());
    }

    @Test
    void toStringContainsFields() {
        GetReplicateInfoResp response = GetReplicateInfoResp.builder()
                .checkpoint(ReplicateCheckpoint.builder().clusterId("c1").build())
                .build();

        String text = response.toString();
        assertEquals("GetReplicateInfoResp{checkpoint=ReplicateCheckpoint{clusterId='c1', pchannel='null', messageID=null, timeTick=null}, salvageCheckpoint=null}", text);
    }
}
