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
import io.milvus.v2.service.cdc.request.DumpMessagesReq;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.DumpMessageInfo;
import io.milvus.v2.service.cdc.response.DumpMessagesResp;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import io.milvus.v2.service.cdc.response.UpdateReplicateConfigurationResp;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CDCService extends BaseService {
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

    public GetReplicateConfigurationResp getReplicateConfiguration(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        GetReplicateConfigurationRequest request = GetReplicateConfigurationRequest.newBuilder().build();

        String title = "GetReplicateConfiguration";

        GetReplicateConfigurationResponse response = blockingStub.getReplicateConfiguration(request);
        rpcUtils.handleResponse(title, response.getStatus());
        return GetReplicateConfigurationResp.builder()
                .replicateConfiguration(ReplicateConfiguration.fromGRPC(response.getConfiguration()))
                .build();
    }

    public UpdateReplicateConfigurationResp updateReplicateConfiguration(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdateReplicateConfigurationReq requestParam) {
        UpdateReplicateConfigurationRequest request = UpdateReplicateConfigurationRequest.newBuilder()
                .setReplicateConfiguration(requestParam.getReplicateConfiguration().toGRPC())
                .setForcePromote(requestParam.isForcePromote())
                .build();

        String title = "UpdateReplicateConfiguration";

        Status response = blockingStub.updateReplicateConfiguration(request);
        rpcUtils.handleResponse(title, response);
        return UpdateReplicateConfigurationResp.builder().build();
    }

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
