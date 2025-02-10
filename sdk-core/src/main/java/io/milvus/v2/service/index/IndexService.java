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

package io.milvus.v2.service.index;

import com.google.gson.JsonObject;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexService extends BaseService {

    public Void createIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateIndexReq request) {
        for(IndexParam indexParam : request.getIndexParams()) {
            String title = String.format("CreateIndexRequest collectionName:%s, fieldName:%s",
                    request.getCollectionName(), indexParam.getFieldName());
            CreateIndexRequest.Builder builder = CreateIndexRequest.newBuilder();
            builder.setCollectionName(request.getCollectionName())
                    .setIndexName(indexParam.getIndexName())
                    .setFieldName(indexParam.getFieldName())
                    .addExtraParams(KeyValuePair.newBuilder()
                            .setKey(Constant.INDEX_TYPE)
                            .setValue(indexParam.getIndexType().getName())
                            .build());

            if (StringUtils.isNotEmpty(request.getDatabaseName())) {
                builder.setDbName(request.getDatabaseName());
            }

            if(indexParam.getMetricType()!= null){
                // only vector field has a metric type
                builder.addExtraParams(KeyValuePair.newBuilder()
                        .setKey(Constant.METRIC_TYPE)
                        .setValue(indexParam.getMetricType().name())
                        .build());
            }
            Map<String, Object> extraParams = indexParam.getExtraParams();
            if (extraParams != null && !extraParams.isEmpty()) {
                JsonObject params = new JsonObject();
                for (String key : extraParams.keySet()) {
                    params.addProperty(key, extraParams.get(key).toString());
                }
                // the extra params is a JSON format string like "{\"M\": 8, \"efConstruction\": 64}"
                builder.addExtraParams(KeyValuePair.newBuilder()
                        .setKey(Constant.PARAMS)
                        .setValue(params.toString())
                        .build());
            }

            Status status = blockingStub.createIndex(builder.build());
            rpcUtils.handleResponse(title, status);
        }

        return null;
    }

    public Void dropIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropIndexReq request) {
        String title = String.format("DropIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName() == null ? "" : request.getFieldName())
                .setIndexName(request.getIndexName() == null ? "" : request.getIndexName())
                .build();

        Status status = blockingStub.dropIndex(dropIndexRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }
    
    public Void alterIndexProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterIndexPropertiesReq request) {
        String title = String.format("AlterIndexPropertiesReq collectionName:%s, indexName:%s",
                request.getCollectionName(), request.getIndexName());
        AlterIndexRequest.Builder builder = AlterIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setIndexName(request.getIndexName());

        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addExtraParams);
        }
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterIndex(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropIndexProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropIndexPropertiesReq request) {
        String title = String.format("DropIndexPropertiesReq collectionName:%s, indexName:%s",
                request.getCollectionName(), request.getIndexName());
        AlterIndexRequest.Builder builder = AlterIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setIndexName(request.getIndexName())
                .addAllDeleteKeys(request.getPropertyKeys());

        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterIndex(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public DescribeIndexResp describeIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeIndexReq request) {
        String title = String.format("DescribeIndexRequest collectionName:%s, fieldName:%s, indexName:%s",
                request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName() == null ? "" : request.getFieldName())
                .setIndexName(request.getIndexName() == null ? "" : request.getIndexName())
                .build();

        DescribeIndexResponse response = blockingStub.describeIndex(describeIndexRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        List<IndexDescription> indexs = response.getIndexDescriptionsList().stream().filter(index -> index.getIndexName().equals(request.getIndexName()) || index.getFieldName().equals(request.getFieldName())).collect(Collectors.toList());
        if (indexs.isEmpty()) {
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Index not found");
        } else if (indexs.size() > 1) {
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, "More than one index found");
        }
        return convertUtils.convertToDescribeIndexResp(indexs);
    }

    public List<String> listIndexes(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListIndexesReq request) {
        String title = String.format("ListIndexesRequest collectionName:%s", request.getCollectionName());
        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        if (request.getFieldName() != null) {
            describeIndexRequest = describeIndexRequest.toBuilder()
                    .setFieldName(request.getFieldName())
                    .build();
        }
        DescribeIndexResponse response = blockingStub.describeIndex(describeIndexRequest);
        // if the collection has no index, return empty list, instead of throwing an exception
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.IndexNotExist ||
                response.getStatus().getCode() == 700) {
            return new ArrayList<>();
        }
        rpcUtils.handleResponse(title, response.getStatus());
        List<String> indexNames = new ArrayList<>();
        response.getIndexDescriptionsList().forEach(index -> {
            indexNames.add(index.getIndexName());
        });
        return indexNames;
    }
}
