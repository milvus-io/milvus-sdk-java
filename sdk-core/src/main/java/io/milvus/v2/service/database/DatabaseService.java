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
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseService extends BaseService {
    public Void createDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateDatabaseReq request) {
        String title = String.format("Create database: '%s'", request.getDatabaseName());
        CreateDatabaseRequest.Builder builder = CreateDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName());
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }

        Status response = blockingStub.createDatabase(builder.build());
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public Void dropDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropDatabaseReq request) {
        String title = String.format("Drop database: '%s'", request.getDatabaseName());
        DropDatabaseRequest rpcRequest = DropDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName())
                .build();

        Status response = blockingStub.dropDatabase(rpcRequest);
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public ListDatabasesResp listDatabases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        ListDatabasesResponse response = blockingStub.listDatabases(ListDatabasesRequest.newBuilder().build());
        rpcUtils.handleResponse("List databases", response.getStatus());
        ListDatabasesResp listDatabasesResp = ListDatabasesResp.builder()
                .databaseNames(response.getDbNamesList())
                .build();

        return listDatabasesResp;
    }

    public Void alterDatabaseProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterDatabasePropertiesReq request) {
        String title = String.format("Alter properties of database: '%s'", request.getDatabaseName());
        AlterDatabaseRequest.Builder builder = AlterDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName());
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }

        Status response = blockingStub.alterDatabase(builder.build());
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public Void dropDatabaseProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropDatabasePropertiesReq request) {
        String title = String.format("Drop properties of database: '%s'", request.getDatabaseName());
        AlterDatabaseRequest.Builder builder = AlterDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName())
                .addAllDeleteKeys(request.getPropertyKeys());

        Status response = blockingStub.alterDatabase(builder.build());
        rpcUtils.handleResponse(title, response);
        return null;
    }

    public DescribeDatabaseResp describeDatabase(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeDatabaseReq request) {
        String title = String.format("Describe database: '%s'", request.getDatabaseName());
        DescribeDatabaseRequest rpcRequest = DescribeDatabaseRequest.newBuilder()
                .setDbName(request.getDatabaseName())
                .build();

        DescribeDatabaseResponse response = blockingStub.describeDatabase(rpcRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        Map<String, String> properties = new HashMap<>();
        response.getPropertiesList().forEach((prop) -> properties.put(prop.getKey(), prop.getValue()));

        DescribeDatabaseResp describeDatabaseResp = DescribeDatabaseResp.builder()
                .databaseName(response.getDbName())
                .properties(properties)
                .build();

        return describeDatabaseResp;
    }
}
