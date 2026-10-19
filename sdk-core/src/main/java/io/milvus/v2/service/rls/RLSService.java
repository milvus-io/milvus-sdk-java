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

package io.milvus.v2.service.rls;

import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.CreateRowPolicyRequest;
import io.milvus.grpc.DeleteRLSPrincipalTagsRequest;
import io.milvus.grpc.DropRowPolicyRequest;
import io.milvus.grpc.GetRLSPrincipalTagsRequest;
import io.milvus.grpc.GetRLSPrincipalTagsResponse;
import io.milvus.grpc.ListRLSPrincipalsRequest;
import io.milvus.grpc.ListRLSPrincipalsResponse;
import io.milvus.grpc.ListRowPoliciesRequest;
import io.milvus.grpc.ListRowPoliciesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.RowPolicy;
import io.milvus.grpc.SetRLSPrincipalTagsRequest;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateRowPolicyRequest;
import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.common.RowPolicyType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.rls.request.CreateRowPolicyReq;
import io.milvus.v2.service.rls.request.DeleteRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.DropRowPolicyReq;
import io.milvus.v2.service.rls.request.GetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.ListRLSPrincipalsReq;
import io.milvus.v2.service.rls.request.ListRowPoliciesReq;
import io.milvus.v2.service.rls.request.SetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.UpdateRowPolicyReq;
import io.milvus.v2.service.rls.response.GetRLSPrincipalTagsResp;
import io.milvus.v2.service.rls.response.ListRLSPrincipalsResp;
import io.milvus.v2.service.rls.response.ListRowPoliciesResp;
import io.milvus.v2.service.rls.response.RowPolicyInfo;
import com.google.gson.reflect.TypeToken;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for row-level security (RLS) operations, including row policy management
 * and principal tags.
 */

public class RLSService extends BaseService {
    /**
     * Creates a row policy for the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the create row policy request
     * @return {@code null}
     */

    public Void createRowPolicy(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateRowPolicyReq request) {
        String title = String.format("Create row policy: '%s' for collection: '%s'", request.getPolicyName(), request.getCollectionName());
        CreateRowPolicyRequest.Builder builder = CreateRowPolicyRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPolicyName(request.getPolicyName())
                .setUsingExpr(request.getUsingExpr())
                .setCheckExpr(request.getCheckExpr())
                .setDescription(request.getDescription());
        if (request.getPolicyType() != null) {
            builder.setPolicyType(io.milvus.grpc.RowPolicyType.forNumber(request.getPolicyType().getCode()));
        }
        if (request.getActions() != null) {
            request.getActions().forEach(action ->
                    builder.addActions(io.milvus.grpc.RowPolicyAction.forNumber(action.getCode())));
        }
        Status status = blockingStub.createRowPolicy(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    /**
     * Updates an existing row policy for the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the update row policy request
     * @return {@code null}
     */

    public Void updateRowPolicy(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdateRowPolicyReq request) {
        String title = String.format("Update row policy: '%s' for collection: '%s'", request.getPolicyName(), request.getCollectionName());
        UpdateRowPolicyRequest.Builder builder = UpdateRowPolicyRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPolicyName(request.getPolicyName())
                .setUsingExpr(request.getUsingExpr())
                .setCheckExpr(request.getCheckExpr())
                .setDescription(request.getDescription());
        if (request.getPolicyType() != null) {
            builder.setPolicyType(io.milvus.grpc.RowPolicyType.forNumber(request.getPolicyType().getCode()));
        }
        if (request.getActions() != null) {
            request.getActions().forEach(action ->
                    builder.addActions(io.milvus.grpc.RowPolicyAction.forNumber(action.getCode())));
        }
        Status status = blockingStub.updateRowPolicy(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    /**
     * Drops the specified row policy.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop row policy request
     * @return {@code null}
     */

    public Void dropRowPolicy(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropRowPolicyReq request) {
        String title = String.format("Drop row policy: '%s' for collection: '%s'", request.getPolicyName(), request.getCollectionName());
        DropRowPolicyRequest dropRowPolicyRequest = DropRowPolicyRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPolicyName(request.getPolicyName())
                .build();
        Status status = blockingStub.dropRowPolicy(dropRowPolicyRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    /**
     * Lists the row policies of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the list row policies request
     * @return the list row policies response
     */

    public ListRowPoliciesResp listRowPolicies(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListRowPoliciesReq request) {
        ListRowPoliciesRequest listRowPoliciesRequest = ListRowPoliciesRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .build();
        ListRowPoliciesResponse response = blockingStub.listRowPolicies(listRowPoliciesRequest);
        rpcUtils.handleResponse("List row policies", response.getStatus());

        List<RowPolicyInfo> policies = response.getPoliciesList().stream()
                .map(this::toRowPolicyInfo)
                .collect(Collectors.toList());
        return ListRowPoliciesResp.builder()
                .dbName(response.getDbName())
                .collectionName(response.getCollectionName())
                .policies(policies)
                .build();
    }

    private RowPolicyInfo toRowPolicyInfo(RowPolicy policy) {
        return RowPolicyInfo.builder()
                .policyName(policy.getPolicyName())
                .policyType(RowPolicyType.fromCode(policy.getPolicyTypeValue()))
                .actions(policy.getActionsValueList().stream()
                        .map(code -> {
                            RowPolicyAction mapped = RowPolicyAction.fromCode(code);
                            return mapped == null ? RowPolicyAction.UNKNOWN : mapped;
                        })
                        .collect(Collectors.toList()))
                .usingExpr(policy.getUsingExpr())
                .checkExpr(policy.getCheckExpr())
                .description(policy.getDescription())
                .build();
    }

    /**
     * Incrementally upserts the tags of the specified principal.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the set RLS principal tags request
     * @return {@code null}
     */

    public Void setRLSPrincipalTags(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, SetRLSPrincipalTagsReq request) {
        String title = String.format("Set RLS principal tags: '%s' for collection: '%s'", request.getPrincipalName(), request.getCollectionName());
        validatePrincipalTags(request.getTags());
        SetRLSPrincipalTagsRequest setRLSPrincipalTagsRequest = SetRLSPrincipalTagsRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPrincipalName(request.getPrincipalName())
                .setTags(JsonUtils.toJson(request.getTags()))
                .build();
        Status status = blockingStub.setRLSPrincipalTags(setRLSPrincipalTagsRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    private void validatePrincipalTags(Map<String, Object> tags) {
        if (tags == null || tags.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "RLS principal tags must be a non-empty mapping");
        }
        for (Map.Entry<String, Object> entry : tags.entrySet()) {
            if (entry.getKey() == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "RLS principal tag keys must be strings");
            }
            Object value = entry.getValue();
            if (value != null && value.getClass().isArray()) {
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    if (!isValidTagValue(java.lang.reflect.Array.get(value, i))) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "RLS principal tag values must be strings, finite numbers, or arrays of them");
                    }
                }
            } else if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (!isValidTagValue(item)) {
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                                "RLS principal tag values must be strings, finite numbers, or arrays of them");
                    }
                }
            } else if (!isValidTagValue(value)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "RLS principal tag values must be strings, finite numbers, or arrays of them");
            }
        }
    }

    private boolean isValidTagValue(Object value) {
        if (value instanceof String) {
            return true;
        }
        if (value instanceof Number) {
            if (value instanceof Double) {
                return !Double.isNaN((Double) value) && !Double.isInfinite((Double) value);
            }
            if (value instanceof Float) {
                return !Float.isNaN((Float) value) && !Float.isInfinite((Float) value);
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the tags of the specified principal.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get RLS principal tags request
     * @return the get RLS principal tags response
     */

    public GetRLSPrincipalTagsResp getRLSPrincipalTags(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetRLSPrincipalTagsReq request) {
        String title = String.format("Get RLS principal tags: '%s' for collection: '%s'", request.getPrincipalName(), request.getCollectionName());
        GetRLSPrincipalTagsRequest getRLSPrincipalTagsRequest = GetRLSPrincipalTagsRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPrincipalName(request.getPrincipalName())
                .build();
        GetRLSPrincipalTagsResponse response = blockingStub.getRLSPrincipalTags(getRLSPrincipalTagsRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        Map<String, Object> tags = JsonUtils.fromJson(response.getTags(), new TypeToken<Map<String, Object>>() {
        }.getType());
        return GetRLSPrincipalTagsResp.builder()
                .tags(tags == null ? new HashMap<>() : tags)
                .build();
    }

    /**
     * Lists the principals that have stored tags in the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the list RLS principals request
     * @return the list RLS principals response
     */

    public ListRLSPrincipalsResp listRLSPrincipals(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListRLSPrincipalsReq request) {
        ListRLSPrincipalsRequest listRLSPrincipalsRequest = ListRLSPrincipalsRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .build();
        ListRLSPrincipalsResponse response = blockingStub.listRLSPrincipals(listRLSPrincipalsRequest);
        rpcUtils.handleResponse("List RLS principals", response.getStatus());

        return ListRLSPrincipalsResp.builder()
                .principalNames(response.getPrincipalNamesList())
                .build();
    }

    /**
     * Deletes the selected tag keys, or all tags when the tag key list is empty.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the delete RLS principal tags request
     * @return {@code null}
     */

    public Void deleteRLSPrincipalTags(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DeleteRLSPrincipalTagsReq request) {
        String title = String.format("Delete RLS principal tags: '%s' for collection: '%s'", request.getPrincipalName(), request.getCollectionName());
        DeleteRLSPrincipalTagsRequest.Builder builder = DeleteRLSPrincipalTagsRequest.newBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setPrincipalName(request.getPrincipalName());
        if (request.getTagKeys() != null) {
            request.getTagKeys().forEach(builder::addTagKeys);
        }
        Status status = blockingStub.deleteRLSPrincipalTags(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }
}
