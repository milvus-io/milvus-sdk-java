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
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.IndexBuildState;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexService extends BaseService {

    public Void createIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateIndexReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        for (IndexParam indexParam : request.getIndexParams()) {
            String fieldName = indexParam.getFieldName();
            String indexName = indexParam.getIndexName();
            String title = String.format("Create index for field: '%s' in collection: '%s' in database: '%s'",
                    fieldName, collectionName, dbName);
            CreateIndexRequest.Builder builder = CreateIndexRequest.newBuilder();
            builder.setCollectionName(collectionName)
                    .setFieldName(fieldName)
                    .addExtraParams(KeyValuePair.newBuilder()
                            .setKey(Constant.INDEX_TYPE)
                            .setValue(indexParam.getIndexType().getName())
                            .build());
            if (StringUtils.isNotEmpty(indexName)) {
                builder.setIndexName(indexName);
            }
            if (StringUtils.isNotEmpty(dbName)) {
                builder.setDbName(dbName);
            }

            if (indexParam.getMetricType() != null) {
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
                WaitForIndexComplete(blockingStub, dbName, collectionName, fieldName, indexName, request.getTimeout());
            }
        }

        return null;
    }

    public Void dropIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropIndexReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String fieldName = request.getFieldName();
        String indexName = request.getIndexName();
        String title = String.format("Drop index in collection: '%s' in database: '%s', fieldName: '%s', indexName: '%s'",
                collectionName, dbName, fieldName, indexName);

        DropIndexRequest.Builder builder = DropIndexRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFieldName(fieldName == null ? "" : fieldName)
                .setIndexName(indexName == null ? "" : indexName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.dropIndex(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void alterIndexProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterIndexPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String indexName = request.getIndexName();
        String title = String.format("Alter properties of index: '%s' in collection: '%s' in database: '%s'",
                indexName, collectionName, dbName);

        AlterIndexRequest.Builder builder = AlterIndexRequest.newBuilder()
                .setCollectionName(collectionName)
                .setIndexName(indexName);
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addExtraParams);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status response = blockingStub.alterIndex(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropIndexProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropIndexPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String indexName = request.getIndexName();
        String title = String.format("Drop properties of index: '%s' in collection: '%s' in database: '%s'",
                indexName, collectionName, dbName);

        AlterIndexRequest.Builder builder = AlterIndexRequest.newBuilder()
                .setCollectionName(collectionName)
                .setIndexName(indexName)
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status response = blockingStub.alterIndex(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public DescribeIndexResp describeIndex(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeIndexReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String fieldName = request.getFieldName();
        String indexName = request.getIndexName();
        String title = String.format("Describe index in collection: '%s' in database: '%s', fieldName: '%s', indexName: '%s'",
                collectionName, dbName, fieldName, indexName);

        DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFieldName(fieldName == null ? "" : fieldName)
                .setIndexName(indexName == null ? "" : indexName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        DescribeIndexResponse response = blockingStub.describeIndex(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        List<IndexDescription> indexes = response.getIndexDescriptionsList().stream().filter(index -> index.getIndexName().equals(request.getIndexName()) || index.getFieldName().equals(request.getFieldName())).collect(Collectors.toList());
        return convertUtils.convertToDescribeIndexResp(indexes);
    }

    public List<String> listIndexes(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListIndexesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String fieldName = request.getFieldName();
        String title = String.format("List indexes in collection: '%s' in database: '%s'", collectionName, dbName);
        DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(fieldName)) {
            builder.setFieldName(fieldName);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        DescribeIndexResponse response = blockingStub.describeIndex(builder.build());
        // if the collection has no index, return empty list, instead of throwing an exception
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.IndexNotExist ||
                response.getStatus().getCode() == 700) {
            return new ArrayList<>();
        }
        rpcUtils.handleResponse(title, response.getStatus());

        return response.getIndexDescriptionsList().stream()
                .filter(desc -> fieldName == null || desc.getFieldName().equals(fieldName))
                .map(IndexDescription::getIndexName)
                .collect(Collectors.toList());
    }

    private void WaitForIndexComplete(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName,
                                      String collectionName, String fieldName, String indexName, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        // alloc a timestamp from the server, the DescribeIndex() will use this timestamp to check the segments
        // which are generated before this timestamp.
        AllocTimestampResponse allocTsResp = blockingStub.allocTimestamp(AllocTimestampRequest.newBuilder().build());
        rpcUtils.handleResponse("Alloc timestamp", allocTsResp.getStatus());
        long serverTs = allocTsResp.getTimestamp();

        while (true) {
            DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                    .databaseName(dbName)
                    .collectionName(collectionName)
                    .fieldName(fieldName)
                    .indexName(indexName)
                    .timestamp(serverTs)
                    .build();
            DescribeIndexResp response = describeIndex(blockingStub, describeIndexReq);
            List<DescribeIndexResp.IndexDesc> descs = response.getIndexDescriptions();
            if (CollectionUtils.isEmpty(descs)) {
                String msg = String.format("No index is found, indexName: '%s' fieldName: '%s' in collection: '%s' in database: '%s'",
                        fieldName, indexName, collectionName, dbName);
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, msg);
            }

            boolean allIndexBuildCompleted = true;
            for (DescribeIndexResp.IndexDesc desc : descs) {
                if (desc.getIndexState() == IndexBuildState.Failed) {
                    throw new MilvusClientException(ErrorCode.SERVER_ERROR,
                            "Index is failed, reason: " + desc.getIndexFailedReason());
                }

                if (desc.getIndexState() != IndexBuildState.Finished) {
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
                logger.error("Thread was interrupted, failed to complete operation");
                return; // or handle interruption appropriately
            }
        }
    }
}
