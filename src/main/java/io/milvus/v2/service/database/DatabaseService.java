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

package io.milvus.v2.service.database;

import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.*;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseService extends BaseService {
    public Void createDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateDatabaseReq request) {
        String title = String.format("CreateDatabaseRequest databaseName:%s", request.getDatabaseName());
        Status response = blockingStub.createDatabase(CreateDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName()).build());
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public Void dropDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropDatabaseReq request) {
        String title = String.format("DropDatabaseRequest databaseName:%s", request.getDatabaseName());
        DropDatabaseRequest rpcRequest = DropDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName())
                .build();

        Status response = blockingStub.dropDatabase(rpcRequest);
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public ListDatabasesResp listDatabases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        ListDatabasesResponse response = blockingStub.listDatabases(ListDatabasesRequest.newBuilder().build());
        ListDatabasesResp listDatabasesResp = ListDatabasesResp.builder()
                .databaseNames(response.getDbNamesList())
                .build();

        return listDatabasesResp;
    }
}
