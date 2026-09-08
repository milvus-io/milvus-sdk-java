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

package io.milvus.unit.v2.service.cdc;

import com.google.protobuf.ByteString;
import io.milvus.grpc.ConnectionParam;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.GetReplicateConfigurationResponse;
import io.milvus.grpc.GetReplicateInfoResponse;
import io.milvus.grpc.ImmutableMessage;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.MessageID;
import io.milvus.grpc.ReplicateCheckpoint;
import io.milvus.grpc.Status;
import io.milvus.grpc.WALName;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.cdc.CDCService;
import io.milvus.v2.service.cdc.request.CrossClusterTopology;
import io.milvus.v2.service.cdc.request.DumpMessagesReq;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.DumpMessagesResp;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class CDCServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private CDCService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new CDCService();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    private static MilvusCluster cluster(String id, String uri) {
        return MilvusCluster.fromGRPC(io.milvus.grpc.MilvusCluster.newBuilder()
                .setClusterId(id)
                .setConnectionParam(ConnectionParam.newBuilder().setUri(uri).build())
                .build());
    }

    @Test
    void getReplicateInfoReturnsCheckpoint() {
        GetReplicateInfoResponse response = GetReplicateInfoResponse.newBuilder()
                .setCheckpoint(ReplicateCheckpoint.newBuilder()
                        .setClusterId("c1")
                        .setPchannel("by-dev-rootcoord-dml_0")
                        .setTimeTick(100L)
                        .build())
                .build();
        when(stub.getReplicateInfo(any())).thenReturn(response);

        GetReplicateInfoResp result = service.getReplicateInfo(stub, GetReplicateInfoReq.builder()
                .sourceClusterId("c1")
                .targetPchannel("by-dev-rootcoord-dml_0")
                .build());

        assertNotNull(result.getCheckpoint());
        assertEquals("c1", result.getCheckpoint().getClusterId());
        assertEquals("by-dev-rootcoord-dml_0", result.getCheckpoint().getPchannel());
        assertEquals(100L, result.getCheckpoint().getTimeTick());
        verify(stub).getReplicateInfo(any());
    }

    @Test
    void getReplicateInfoRejectsEmptySourceClusterId() {
        GetReplicateInfoReq request = GetReplicateInfoReq.builder()
                .sourceClusterId("")
                .targetPchannel("p")
                .build();

        assertThrows(MilvusClientException.class, () -> service.getReplicateInfo(stub, request));
    }

    @Test
    void getReplicateInfoRejectsEmptyTargetPchannel() {
        GetReplicateInfoReq request = GetReplicateInfoReq.builder()
                .sourceClusterId("c1")
                .targetPchannel(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.getReplicateInfo(stub, request));
    }

    @Test
    void getReplicateConfigurationReturnsConfiguration() {
        GetReplicateConfigurationResponse response = GetReplicateConfigurationResponse.newBuilder()
                .setStatus(success())
                .setConfiguration(io.milvus.grpc.ReplicateConfiguration.newBuilder()
                        .addClusters(io.milvus.grpc.MilvusCluster.newBuilder()
                                .setClusterId("c1")
                                .setConnectionParam(ConnectionParam.newBuilder().setUri("http://c1:19530").build())
                                .build())
                        .build())
                .build();
        when(stub.getReplicateConfiguration(any())).thenReturn(response);

        GetReplicateConfigurationResp result = service.getReplicateConfiguration(stub);

        assertEquals("c1", result.getReplicateConfiguration().getClusters().get(0).getClusterId());
        assertEquals("http://c1:19530", result.getReplicateConfiguration().getClusters().get(0).getUri());
    }

    @Test
    void updateReplicateConfigurationReturnsEmptyResp() {
        when(stub.updateReplicateConfiguration(any())).thenReturn(success());
        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(Collections.singletonList(cluster("c1", "http://c1:19530")))
                .build();
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .forcePromote(true)
                .build();

        assertNotNull(service.updateReplicateConfiguration(stub, request));
        verify(stub).updateReplicateConfiguration(any());
    }

    @Test
    void updateReplicateConfigurationRejectsNullConfiguration() {
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.updateReplicateConfiguration(stub, request));
    }

    @Test
    void updateReplicateConfigurationRejectsEmptyClusters() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(Collections.emptyList())
                .build();
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .build();

        assertThrows(MilvusClientException.class, () -> service.updateReplicateConfiguration(stub, request));
    }

    @Test
    void updateReplicateConfigurationRejectsEmptyClusterUri() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(Collections.singletonList(cluster("c1", "")))
                .build();
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .build();

        assertThrows(MilvusClientException.class, () -> service.updateReplicateConfiguration(stub, request));
    }

    @Test
    void updateReplicateConfigurationRejectsIncompleteTopology() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(Collections.singletonList(cluster("c1", "http://c1:19530")))
                .crossClusterTopologies(Collections.singletonList(
                        CrossClusterTopology.builder().sourceClusterId("c1").build()))
                .build();
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .build();

        assertThrows(MilvusClientException.class, () -> service.updateReplicateConfiguration(stub, request));
    }

    @Test
    void dumpMessagesStreamsMessagesAndSkipsStatus() {
        DumpMessagesResponse statusResponse = DumpMessagesResponse.newBuilder()
                .setStatus(success())
                .build();
        DumpMessagesResponse messageResponse = DumpMessagesResponse.newBuilder()
                .setMessage(ImmutableMessage.newBuilder()
                        .setId(MessageID.newBuilder().setId("m1").setWALName(WALName.Kafka).build())
                        .setPayload(ByteString.copyFromUtf8("payload"))
                        .putProperties("k", "v")
                        .build())
                .build();
        when(stub.dumpMessages(any())).thenReturn(Arrays.asList(statusResponse, messageResponse).iterator());

        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("p")
                .startMessageID(GetReplicateInfoResp.MessageID.builder()
                        .id("m1")
                        .walName("Kafka")
                        .build())
                .startTimetick(1L)
                .endTimetick(2L)
                .includeStartMessage(true)
                .build();

        DumpMessagesResp result = service.dumpMessages(stub, request);

        int count = 0;
        for (io.milvus.v2.service.cdc.response.DumpMessageInfo message : result.getMessages()) {
            count++;
            assertEquals("m1", message.getMessageID().getId());
            assertEquals("Kafka", message.getMessageID().getWalName());
            assertArrayEquals("payload".getBytes(), message.getPayload());
            assertEquals("v", message.getProperties().get("k"));
        }
        assertEquals(1, count);
        verify(stub).dumpMessages(any());
    }

    @Test
    void dumpMessagesRejectsEmptyPchannel() {
        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("")
                .startMessageID(GetReplicateInfoResp.MessageID.builder().id("m1").walName("Kafka").build())
                .build();

        assertThrows(MilvusClientException.class, () -> service.dumpMessages(stub, request));
    }

    @Test
    void dumpMessagesRejectsNullStartMessageId() {
        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("p")
                .startMessageID(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.dumpMessages(stub, request));
    }

    @Test
    void dumpMessagesRejectsInvalidWalName() {
        DumpMessagesReq request = DumpMessagesReq.builder()
                .pchannel("p")
                .startMessageID(GetReplicateInfoResp.MessageID.builder().id("m1").walName("FooBar").build())
                .build();

        assertThrows(MilvusClientException.class, () -> service.dumpMessages(stub, request));
    }
}
