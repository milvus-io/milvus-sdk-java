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

import io.milvus.grpc.DumpMessagesRequest;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.GetReplicateConfigurationRequest;
import io.milvus.grpc.GetReplicateConfigurationResponse;
import io.milvus.grpc.GetReplicateInfoRequest;
import io.milvus.grpc.GetReplicateInfoResponse;
import io.milvus.grpc.ImmutableMessage;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.MessageID;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateReplicateConfigurationRequest;
import io.milvus.grpc.WALName;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.cdc.request.CrossClusterTopology;
import io.milvus.v2.service.cdc.request.DumpMessagesReq;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.DumpMessageInfo;
import io.milvus.v2.service.cdc.response.DumpMessagesResp;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import io.milvus.v2.service.cdc.response.UpdateReplicateConfigurationResp;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Service for CDC (change data capture) operations, such as querying replication information
 * and dumping messages.
 */
public class CDCService extends BaseService {
    /**
     * Returns the replication information for the given source cluster and target physical channel.
     *
     * @param blockingStub the gRPC blocking stub
     * @param requestParam the get replicate info request
     * @return the get replicate info response
     */
    public GetReplicateInfoResp getReplicateInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetReplicateInfoReq requestParam) {
        if (StringUtils.isEmpty(requestParam.getSourceClusterId())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "sourceClusterId cannot be null or empty");
        }
        if (StringUtils.isEmpty(requestParam.getTargetPchannel())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "targetPchannel cannot be null or empty");
        }

        GetReplicateInfoRequest request = GetReplicateInfoRequest.newBuilder()
                .setSourceClusterId(requestParam.getSourceClusterId())
                .setTargetPchannel(requestParam.getTargetPchannel())
                .build();

        GetReplicateInfoResponse response = blockingStub.getReplicateInfo(request);
        return GetReplicateInfoResp.builder()
                .checkpoint(response.hasCheckpoint() ? GetReplicateInfoResp.ReplicateCheckpoint.fromGRPC(response.getCheckpoint()) : null)
                .salvageCheckpoint(response.hasSalvageCheckpoint() ? GetReplicateInfoResp.ReplicateCheckpoint.fromGRPC(response.getSalvageCheckpoint()) : null)
                .build();
    }

    /**
     * Returns the current replication configuration.
     *
     * @param blockingStub the gRPC blocking stub
     * @return the get replicate configuration response
     */
    public GetReplicateConfigurationResp getReplicateConfiguration(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        GetReplicateConfigurationRequest request = GetReplicateConfigurationRequest.newBuilder().build();

        String title = "GetReplicateConfiguration";

        GetReplicateConfigurationResponse response = blockingStub.getReplicateConfiguration(request);
        rpcUtils.handleResponse(title, response.getStatus());
        return GetReplicateConfigurationResp.builder()
                .replicateConfiguration(ReplicateConfiguration.fromGRPC(response.getConfiguration()))
                .build();
    }

    /**
     * Updates the replication configuration, optionally forcing a promotion of the replica.
     *
     * @param blockingStub the gRPC blocking stub
     * @param requestParam the update replicate configuration request
     * @return the update replicate configuration response
     */
    public UpdateReplicateConfigurationResp updateReplicateConfiguration(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdateReplicateConfigurationReq requestParam) {
        ReplicateConfiguration configuration = requestParam.getReplicateConfiguration();
        if (configuration == null || CollectionUtils.isEmpty(configuration.getClusters())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "replicate configuration must contain at least one cluster");
        }
        for (MilvusCluster cluster : configuration.getClusters()) {
            if (StringUtils.isEmpty(cluster.getClusterId())) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "clusterId cannot be null or empty");
            }
            if (StringUtils.isEmpty(cluster.getUri())) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "connection uri cannot be null or empty");
            }
        }
        List<CrossClusterTopology> topologies = configuration.getCrossClusterTopologies();
        if (topologies != null) {
            for (CrossClusterTopology topology : topologies) {
                if (StringUtils.isEmpty(topology.getSourceClusterId()) || StringUtils.isEmpty(topology.getTargetClusterId())) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "cross cluster topology requires both source_cluster_id and target_cluster_id");
                }
            }
        }
        UpdateReplicateConfigurationRequest request = UpdateReplicateConfigurationRequest.newBuilder()
                .setReplicateConfiguration(configuration.toGRPC())
                .setForcePromote(requestParam.isForcePromote())
                .build();

        String title = "UpdateReplicateConfiguration";

        Status response = blockingStub.updateReplicateConfiguration(request);
        rpcUtils.handleResponse(title, response);
        return UpdateReplicateConfigurationResp.builder().build();
    }

    /**
     * Dumps messages from the given physical channel, optionally bounded by a start/end timetick,
     * returning an iterable stream of messages.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the dump messages request
     * @return the dump messages response containing the message stream
     */
    public DumpMessagesResp dumpMessages(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                         DumpMessagesReq request) {
        if (StringUtils.isEmpty(request.getPchannel())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "pchannel cannot be null or empty");
        }
        if (request.getStartMessageID() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "startMessageID cannot be null");
        }
        if (StringUtils.isEmpty(request.getStartMessageID().getId())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "startMessageID.id cannot be null or empty");
        }
        if (StringUtils.isEmpty(request.getStartMessageID().getWalName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "startMessageID.walName cannot be null or empty");
        }

        WALName walName;
        try {
            walName = WALName.valueOf(request.getStartMessageID().getWalName());
        } catch (IllegalArgumentException e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Invalid walName: " + request.getStartMessageID().getWalName());
        }

        DumpMessagesRequest grpcRequest = DumpMessagesRequest.newBuilder()
                .setPchannel(request.getPchannel())
                .setStartMessageId(MessageID.newBuilder()
                        .setId(request.getStartMessageID().getId())
                        .setWALName(walName)
                        .build())
                .setStartTimetick(request.getStartTimetick())
                .setEndTimetick(request.getEndTimetick())
                .setIncludeStartMessage(Boolean.TRUE.equals(request.getIncludeStartMessage()))
                .build();

        Iterator<DumpMessagesResponse> responseIterator = blockingStub.dumpMessages(grpcRequest);
        Iterable<DumpMessageInfo> messages = () -> new Iterator<DumpMessageInfo>() {
            private DumpMessageInfo nextMessage;
            private boolean nextReady;

            @Override
            public boolean hasNext() {
                if (nextReady) {
                    return true;
                }
                while (responseIterator.hasNext()) {
                    DumpMessagesResponse response = responseIterator.next();
                    switch (response.getResponseCase()) {
                        case STATUS:
                            rpcUtils.handleResponse("DumpMessages", response.getStatus());
                            continue;
                        case MESSAGE:
                            nextMessage = convertDumpMessage(response.getMessage());
                            nextReady = true;
                            return true;
                        case RESPONSE_NOT_SET:
                        default:
                            throw new MilvusClientException(ErrorCode.CLIENT_ERROR,
                                    "unexpected DumpMessagesResponse oneof arm: " + response.getResponseCase());
                    }
                }
                return false;
            }

            @Override
            public DumpMessageInfo next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more dump messages");
                }
                DumpMessageInfo current = nextMessage;
                nextMessage = null;
                nextReady = false;
                return current;
            }
        };

        return DumpMessagesResp.builder()
                .messages(messages)
                .build();
    }

    private DumpMessageInfo convertDumpMessage(ImmutableMessage message) {
        return DumpMessageInfo.builder()
                .messageID(message.hasId() ? GetReplicateInfoResp.MessageID.fromGRPC(message.getId()) : null)
                .payload(message.getPayload().toByteArray())
                .properties(new HashMap<>(message.getPropertiesMap()))
                .build();
    }
}
