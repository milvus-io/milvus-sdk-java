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

import io.milvus.grpc.GetReplicateConfigurationRequest;
import io.milvus.grpc.GetReplicateConfigurationResponse;
import io.milvus.grpc.GetReplicateInfoRequest;
import io.milvus.grpc.GetReplicateInfoResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateReplicateConfigurationRequest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;
import io.milvus.v2.service.cdc.response.UpdateReplicateConfigurationResp;
import org.apache.commons.lang3.StringUtils;

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
}
