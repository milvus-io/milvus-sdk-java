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

import io.milvus.grpc.AllocTimestampRequest;
import io.milvus.grpc.AllocTimestampResponse;
import io.milvus.grpc.AlterIndexRequest;
import io.milvus.grpc.CreateIndexRequest;
import io.milvus.grpc.DescribeIndexRequest;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.DropIndexRequest;
import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexPropertiesReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
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
                    .setFieldName(indexParam.getFieldName())
                    .addExtraParams(KeyValuePair.newBuilder()
                            .setKey(Constant.INDEX_TYPE)
                            .setValue(indexParam.getIndexType().getName())
                            .build());

            if (StringUtils.isNotEmpty(indexParam.getIndexName())) {
                builder.setIndexName(indexParam.getIndexName());
            }

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
                for (String key : extraParams.keySet()) {
                    builder.addExtraParams(KeyValuePair.newBuilder()
                            .setKey(key)
                            .setValue(extraParams.get(key).toString())
                            .build());
                }
            }

            Status status = blockingStub.createIndex(builder.build());
            rpcUtils.handleResponse(title, status);
            if (request.getSync()) {
                WaitForIndexComplete(blockingStub, request.getDatabaseName(), request.getCollectionName(), indexParam.getFieldName(),
                        indexParam.getIndexName(), request.getTimeout());
            }
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
        String title = String.format("DescribeIndexRequest databaseName:%s collectionName:%s, fieldName:%s, indexName:%s",
               request.getDatabaseName(), request.getCollectionName(), request.getFieldName(), request.getIndexName());
        DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName() == null ? "" : request.getFieldName())
                .setIndexName(request.getIndexName() == null ? "" : request.getIndexName());
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        DescribeIndexResponse response = blockingStub.describeIndex(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        List<IndexDescription> indexs = response.getIndexDescriptionsList().stream().filter(index -> index.getIndexName().equals(request.getIndexName()) || index.getFieldName().equals(request.getFieldName())).collect(Collectors.toList());
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

        return response.getIndexDescriptionsList().stream()
                .filter(desc -> request.getFieldName() == null || desc.getFieldName().equals(request.getFieldName()))
                .map(IndexDescription::getIndexName)
                .collect(Collectors.toList());
    }

    private void WaitForIndexComplete(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                     String databaseName, String collectionName, String fieldName, String indexName, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        // alloc a timestamp from the server, the DescribeIndex() will use this timestamp to check the segments
        // which are generated before this timestamp.
        AllocTimestampResponse allocTsResp = blockingStub.allocTimestamp(AllocTimestampRequest.newBuilder().build());
        rpcUtils.handleResponse("AllocTimestampRequest", allocTsResp.getStatus());
        long serverTs = allocTsResp.getTimestamp();

        while (true) {
            DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                    .collectionName(collectionName)
                    .fieldName(fieldName)
                    .indexName(indexName)
                    .timestamp(serverTs)
                    .build();
            if (StringUtils.isNotEmpty(databaseName)) {
                describeIndexReq.setDatabaseName(databaseName);
            }
            DescribeIndexResp response = describeIndex(blockingStub, describeIndexReq);
            List<DescribeIndexResp.IndexDesc> indices = response.getIndexDescriptions();
            if (CollectionUtils.isEmpty(indices)) {
                String msg = String.format("Failed to describe the index '%s' of field '%s' from serv side", fieldName, indexName);
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, msg);
            }

            boolean allIndexBuildCompleted = true;
            for (DescribeIndexResp.IndexDesc index : indices) {
                if (index.getIndexState() == IndexBuildState.Failed) {
                    String msg = "Index is failed, reason: " + index.getIndexFailedReason();
                    throw new MilvusClientException(ErrorCode.SERVER_ERROR, msg);
                }

                if (index.getIndexState() != IndexBuildState.Finished) {
                    allIndexBuildCompleted = false;
                }
            }

            if (allIndexBuildCompleted) {
                return;
            }

            // Check if timeout is exceeded
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Create index timeout");
            }
            // Wait for a certain period before checking again
            try {
                Thread.sleep(500); // Sleep for 0.5 second. Adjust this value as needed.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Thread was interrupted, failed to complete operation");
                return; // or handle interruption appropriately
            }
        }
    }
}
