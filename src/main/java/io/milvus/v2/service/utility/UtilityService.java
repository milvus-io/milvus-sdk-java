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

package io.milvus.v2.service.utility;

import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.common.BulkInsertState;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UtilityService extends BaseService {
    public R<RpcStatus> flush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushReq request) {
        String title = String.format("Flush collection %s", request.getCollectionName());
        io.milvus.grpc.FlushRequest flushRequest = io.milvus.grpc.FlushRequest.newBuilder()
                .addCollectionNames(request.getCollectionName())
                .build();
        FlushResponse status = blockingStub.flush(flushRequest);
        rpcUtils.handleResponse(title, status.getStatus());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public Void createAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateAliasReq request) {
        String title = String.format("Create alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.CreateAliasRequest createAliasRequest = io.milvus.grpc.CreateAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.createAlias(createAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void dropAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropAliasReq request) {
        String title = String.format("Drop alias %s", request.getAlias());
        io.milvus.grpc.DropAliasRequest dropAliasRequest = io.milvus.grpc.DropAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.dropAlias(dropAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void alterAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterAliasReq request) {
        String title = String.format("Alter alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.AlterAliasRequest alterAliasRequest = io.milvus.grpc.AlterAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.alterAlias(alterAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public DescribeAliasResp describeAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeAliasReq request) {
        String title = String.format("Describe alias %s", request.getAlias());
        io.milvus.grpc.DescribeAliasRequest describeAliasRequest = io.milvus.grpc.DescribeAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.DescribeAliasResponse response = blockingStub.describeAlias(describeAliasRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return DescribeAliasResp.builder()
                .collectionName(response.getCollection())
                .alias(response.getAlias())
                .build();
    }

    public ListAliasResp listAliases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListAliasesReq request) {
        String title = "List aliases";
        io.milvus.grpc.ListAliasesRequest listAliasesRequest = io.milvus.grpc.ListAliasesRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        io.milvus.grpc.ListAliasesResponse response = blockingStub.listAliases(listAliasesRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return ListAliasResp.builder()
                .collectionName(response.getCollectionName())
                .alias(response.getAliasesList())
                .build();
    }

    public BulkInsertResp bulkInsert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, BulkInsertReq request) {
        String title = "Bulk insert";
        io.milvus.grpc.ImportRequest importRequest = io.milvus.grpc.ImportRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName())
                .addAllFiles(request.getFiles())
                .build();
        io.milvus.grpc.ImportResponse response = blockingStub.import_(importRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return BulkInsertResp.builder()
                .tasks(response.getTasksList())
                .build();
    }

    public GetBulkInsertStateResp getBulkInsertState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                     GetBulkInsertStateReq request) {
        String title = "Get bulkinsert state";
        io.milvus.grpc.GetImportStateRequest getRequest = io.milvus.grpc.GetImportStateRequest.newBuilder()
                .setTask(request.getTaskID())
                .build();
        io.milvus.grpc.GetImportStateResponse response = blockingStub.getImportState(getRequest);

        rpcUtils.handleResponse(title, response.getStatus());
        return convertImportStateResponse(response);
    }

    private GetBulkInsertStateResp convertImportStateResponse(io.milvus.grpc.GetImportStateResponse response) {
        Map<String, String> infos = new HashMap<>();
        response.getInfosList().forEach((kv)->{infos.put(kv.getKey(), kv.getValue());});

        return GetBulkInsertStateResp.builder()
                .taskID(response.getId())
                .collectionID(response.getCollectionId())
                .segmentIDs(response.getSegmentIdsList())
                .state(BulkInsertState.valueOf(response.getState().name()))
                .rowCount(response.getRowCount())
                .infos(infos)
                .build();
    }

    public ListBulkInsertTasksResp listBulkInsertTasks(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                       ListBulkInsertTasksReq request) {
        String title = "List bulkinsert tasks";
        io.milvus.grpc.ListImportTasksRequest listRequest = io.milvus.grpc.ListImportTasksRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setLimit(request.getLimit())
                .build();
        io.milvus.grpc.ListImportTasksResponse response = blockingStub.listImportTasks(listRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        List<GetBulkInsertStateResp> tasks = new ArrayList<>();
        response.getTasksList().forEach((task)-> tasks.add(convertImportStateResponse(task)));

        return ListBulkInsertTasksResp.builder()
                .tasks(tasks)
                .build();
    }
}

