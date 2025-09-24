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

import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateReplicateConfigurationRequest;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.UpdateReplicateConfigurationResp;

public class CDCService extends BaseService {
    public UpdateReplicateConfigurationResp updateReplicateConfiguration(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdateReplicateConfigurationReq requestParam) {
        UpdateReplicateConfigurationRequest request = UpdateReplicateConfigurationRequest.newBuilder()
                .setReplicateConfiguration(requestParam.getReplicateConfiguration().toGRPC())
                .build();

        String title = "UpdateReplicateConfiguration";

        Status response = blockingStub.updateReplicateConfiguration(request);
        rpcUtils.handleResponse(title, response);
        return UpdateReplicateConfigurationResp.builder().build();
    }
}
