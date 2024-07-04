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

import io.milvus.grpc.*;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IndexService extends BaseService {

    public Void createIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateIndexReq request) {
        for(IndexParam indexParam : request.getIndexParams()) {
            String title = String.format("CreateIndexRequest collectionName:%s, fieldName:%s",
                    request.getCollectionName(), indexParam.getFieldName());
            CreateIndexRequest createIndexRequest = CreateIndexRequest.newBuilder()
                    .setCollectionName(request.getCollectionName())
                    .setIndexName(indexParam.getIndexName())
                    .setFieldName(indexParam.getFieldName())
                    .addExtraParams(KeyValuePair.newBuilder()
                            .setKey("index_type")
                            .setValue(indexParam.getIndexType().getName())
                            .build())
                    .build();
            if(indexParam.getMetricType()!= null){
                // only vector field has a metric type
                createIndexRequest = createIndexRequest.toBuilder()
                        .addExtraParams(KeyValuePair.newBuilder()
                                .setKey("metric_type")
                                .setValue(indexParam.getMetricType().name())
                                .build())
                        .build();
            }
            if (indexParam.getExtraParams() != null) {
                for (String key : indexParam.getExtraParams().keySet()) {
                    createIndexRequest = createIndexRequest.toBuilder()
                            .addExtraParams(KeyValuePair.newBuilder()
                                    .setKey(key)
                                    .setValue(String.valueOf(indexParam.getExtraParams().get(key)))
                                    .build())
                            .build();
                }
            }

            Status status = blockingStub.createIndex(createIndexRequest);
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
        rpcUtils.handleResponse(title, response.getStatus());
        List<String> indexNames = new ArrayList<>();
        response.getIndexDescriptionsList().forEach(index -> {
            indexNames.add(index.getIndexName());
        });
        return indexNames;
    }
}
