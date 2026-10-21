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

package io.milvus.integration.v2.service.rls;

import io.milvus.grpc.CreateRowPolicyRequest;
import io.milvus.grpc.DeleteRLSPrincipalTagsRequest;
import io.milvus.grpc.DropRowPolicyRequest;
import io.milvus.grpc.GetRLSPrincipalTagsRequest;
import io.milvus.grpc.ListRLSPrincipalsRequest;
import io.milvus.grpc.ListRowPoliciesRequest;
import io.milvus.grpc.ListRowPoliciesResponse;
import io.milvus.grpc.RowPolicy;
import io.milvus.grpc.RowPolicyType;
import io.milvus.grpc.SetRLSPrincipalTagsRequest;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateRowPolicyRequest;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.exception.MilvusClientException;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class RLSTest extends BaseTest {

    @Test
    void testCreateRowPolicy() {
        CreateRowPolicyReq request = CreateRowPolicyReq.builder()
                .databaseName("default")
                .collectionName("test")
                .policyName("policy_test")
                .policyType(io.milvus.v2.common.RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY, RowPolicyAction.SEARCH))
                .usingExpr("user_id == 1")
                .checkExpr("user_id == 1")
                .description("test row policy")
                .build();
        client_v2.createRowPolicy(request);

        ArgumentCaptor<CreateRowPolicyRequest> captor = ArgumentCaptor.forClass(CreateRowPolicyRequest.class);
        verify(blockingStub).createRowPolicy(captor.capture());
        CreateRowPolicyRequest value = captor.getValue();
        assertEquals("default", value.getDbName());
        assertEquals("test", value.getCollectionName());
        assertEquals("policy_test", value.getPolicyName());
        assertEquals(RowPolicyType.RowPolicyTypePermissive, value.getPolicyType());
        assertEquals(2, value.getActionsCount());
        assertEquals(io.milvus.grpc.RowPolicyAction.Query, value.getActions(0));
        assertEquals(io.milvus.grpc.RowPolicyAction.Search, value.getActions(1));
        assertEquals("user_id == 1", value.getUsingExpr());
        assertEquals("user_id == 1", value.getCheckExpr());
        assertEquals("test row policy", value.getDescription());
    }

    @Test
    void testUpdateRowPolicy() {
        UpdateRowPolicyReq request = UpdateRowPolicyReq.builder()
                .databaseName("default")
                .collectionName("test")
                .policyName("policy_test")
                .policyType(io.milvus.v2.common.RowPolicyType.RESTRICTIVE)
                .actions(Arrays.asList(RowPolicyAction.INSERT, RowPolicyAction.UPSERT))
                .usingExpr("team == \"a\"")
                .build();
        client_v2.updateRowPolicy(request);

        ArgumentCaptor<UpdateRowPolicyRequest> captor = ArgumentCaptor.forClass(UpdateRowPolicyRequest.class);
        verify(blockingStub).updateRowPolicy(captor.capture());
        UpdateRowPolicyRequest value = captor.getValue();
        assertEquals("default", value.getDbName());
        assertEquals("test", value.getCollectionName());
        assertEquals("policy_test", value.getPolicyName());
        assertEquals(RowPolicyType.RowPolicyTypeRestrictive, value.getPolicyType());
        assertEquals(2, value.getActionsCount());
        assertEquals(io.milvus.grpc.RowPolicyAction.Insert, value.getActions(0));
        assertEquals(io.milvus.grpc.RowPolicyAction.Upsert, value.getActions(1));
        assertEquals("team == \"a\"", value.getUsingExpr());
    }

    @Test
    void testDropRowPolicy() {
        DropRowPolicyReq request = DropRowPolicyReq.builder()
                .databaseName("default")
                .collectionName("test")
                .policyName("policy_test")
                .build();
        client_v2.dropRowPolicy(request);

        ArgumentCaptor<DropRowPolicyRequest> captor = ArgumentCaptor.forClass(DropRowPolicyRequest.class);
        verify(blockingStub).dropRowPolicy(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());
        assertEquals("policy_test", captor.getValue().getPolicyName());
    }

    @Test
    void testListRowPolicies() {
        ListRowPoliciesReq request = ListRowPoliciesReq.builder()
                .databaseName("default")
                .collectionName("test")
                .build();
        ListRowPoliciesResp resp = client_v2.listRowPolicies(request);

        ArgumentCaptor<ListRowPoliciesRequest> captor = ArgumentCaptor.forClass(ListRowPoliciesRequest.class);
        verify(blockingStub).listRowPolicies(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());

        assertEquals("default", resp.getDbName());
        assertEquals("test", resp.getCollectionName());
        assertNotNull(resp.getPolicies());
        assertEquals(1, resp.getPolicies().size());
        RowPolicyInfo policy = resp.getPolicies().get(0);
        assertEquals("policy_test", policy.getPolicyName());
        assertEquals(io.milvus.v2.common.RowPolicyType.PERMISSIVE, policy.getPolicyType());
        assertEquals(2, policy.getActions().size());
        assertEquals(RowPolicyAction.QUERY, policy.getActions().get(0));
        assertEquals(RowPolicyAction.SEARCH, policy.getActions().get(1));
        assertEquals("user_id == 1", policy.getUsingExpr());
        assertEquals("user_id == 1", policy.getCheckExpr());
        assertEquals("test row policy", policy.getDescription());
    }

    @Test
    void testSetRLSPrincipalTags() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("team", "a");
        tags.put("level", 1);
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();
        client_v2.setRLSPrincipalTags(request);

        ArgumentCaptor<SetRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(SetRLSPrincipalTagsRequest.class);
        verify(blockingStub).setRLSPrincipalTags(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());
        assertEquals("alice", captor.getValue().getPrincipalName());
        assertEquals("{\"team\":\"a\",\"level\":1}", captor.getValue().getTags());
    }

    @Test
    void testGetRLSPrincipalTags() {
        GetRLSPrincipalTagsReq request = GetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .build();
        GetRLSPrincipalTagsResp resp = client_v2.getRLSPrincipalTags(request);

        ArgumentCaptor<GetRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(GetRLSPrincipalTagsRequest.class);
        verify(blockingStub).getRLSPrincipalTags(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());
        assertEquals("alice", captor.getValue().getPrincipalName());

        assertNotNull(resp.getTags());
        assertEquals("a", resp.getTags().get("team"));
        assertEquals(1L, resp.getTags().get("level"));
    }

    @Test
    void testListRLSPrincipals() {
        ListRLSPrincipalsReq request = ListRLSPrincipalsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .build();
        ListRLSPrincipalsResp resp = client_v2.listRLSPrincipals(request);

        ArgumentCaptor<ListRLSPrincipalsRequest> captor = ArgumentCaptor.forClass(ListRLSPrincipalsRequest.class);
        verify(blockingStub).listRLSPrincipals(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());

        assertNotNull(resp.getPrincipalNames());
        assertEquals(2, resp.getPrincipalNames().size());
        assertEquals("alice", resp.getPrincipalNames().get(0));
        assertEquals("bob", resp.getPrincipalNames().get(1));
    }

    @Test
    void testDeleteRLSPrincipalTags() {
        DeleteRLSPrincipalTagsReq request = DeleteRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tagKeys(Arrays.asList("team", "level"))
                .build();
        client_v2.deleteRLSPrincipalTags(request);

        ArgumentCaptor<DeleteRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(DeleteRLSPrincipalTagsRequest.class);
        verify(blockingStub).deleteRLSPrincipalTags(captor.capture());
        assertEquals("default", captor.getValue().getDbName());
        assertEquals("test", captor.getValue().getCollectionName());
        assertEquals("alice", captor.getValue().getPrincipalName());
        assertEquals(2, captor.getValue().getTagKeysCount());
        assertEquals("team", captor.getValue().getTagKeys(0));
        assertEquals("level", captor.getValue().getTagKeys(1));
    }

    @Test
    void testDeleteRLSPrincipalTagsWithEmptyKeysDeletesAll() {
        DeleteRLSPrincipalTagsReq request = DeleteRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .build();
        client_v2.deleteRLSPrincipalTags(request);

        ArgumentCaptor<DeleteRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(DeleteRLSPrincipalTagsRequest.class);
        verify(blockingStub).deleteRLSPrincipalTags(captor.capture());
        assertEquals(0, captor.getValue().getTagKeysCount());
    }

    @Test
    void testDeleteRLSPrincipalTagsWithNullKeysDeletesAll() {
        DeleteRLSPrincipalTagsReq request = DeleteRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .build();
        request.setTagKeys(null);
        client_v2.deleteRLSPrincipalTags(request);

        ArgumentCaptor<DeleteRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(DeleteRLSPrincipalTagsRequest.class);
        verify(blockingStub).deleteRLSPrincipalTags(captor.capture());
        assertEquals(0, captor.getValue().getTagKeysCount());
    }

    @Test
    void testGetRLSPrincipalTagsWithEmptyTagsReturnsEmptyMap() {
        when(blockingStub.getRLSPrincipalTags(any())).thenReturn(
                io.milvus.grpc.GetRLSPrincipalTagsResponse.newBuilder()
                        .setStatus(Status.newBuilder().setCode(0).build())
                        .setTags("")
                        .build());

        GetRLSPrincipalTagsReq request = GetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .build();
        GetRLSPrincipalTagsResp resp = client_v2.getRLSPrincipalTags(request);

        assertNotNull(resp.getTags());
        assertTrue(resp.getTags().isEmpty());
    }

    @Test
    void testSetRLSPrincipalTagsRejectsEmptyTags() {
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(new LinkedHashMap<>())
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsInvalidTagValue() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("active", Boolean.TRUE);
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsNonFiniteNumber() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("score", Double.NaN);
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsNullKey() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put(null, "value");
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsInvalidArrayElement() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("flags", new Object[]{Boolean.TRUE});
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsInvalidListElement() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("flags", java.util.Arrays.asList("a", Boolean.TRUE));
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testSetRLSPrincipalTagsRejectsNullValue() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("team", null);
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();

        assertThrows(MilvusClientException.class, () -> client_v2.setRLSPrincipalTags(request));
    }

    @Test
    void testListRowPoliciesMapsUnknownActionToUnknown() {
        when(blockingStub.listRowPolicies(any())).thenReturn(
                ListRowPoliciesResponse.newBuilder()
                        .setStatus(Status.newBuilder().setCode(0).build())
                        .setDbName("default")
                        .setCollectionName("test")
                        .addPolicies(RowPolicy.newBuilder()
                                .setPolicyName("policy_unknown")
                                .setPolicyType(RowPolicyType.RowPolicyTypePermissive)
                                .addActionsValue(99)
                                .build())
                        .build());

        ListRowPoliciesResp resp = client_v2.listRowPolicies(ListRowPoliciesReq.builder()
                .databaseName("default")
                .collectionName("test")
                .build());

        assertEquals(1, resp.getPolicies().size());
        assertEquals(RowPolicyAction.UNKNOWN, resp.getPolicies().get(0).getActions().get(0));
    }

    @Test
    void testSetRLSPrincipalTagsAcceptsArrayValue() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("teams", new String[]{"a", "b"});
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();
        client_v2.setRLSPrincipalTags(request);

        ArgumentCaptor<SetRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(SetRLSPrincipalTagsRequest.class);
        verify(blockingStub).setRLSPrincipalTags(captor.capture());
        assertEquals("alice", captor.getValue().getPrincipalName());
        assertEquals("{\"teams\":[\"a\",\"b\"]}", captor.getValue().getTags());
    }

    @Test
    void testSetRLSPrincipalTagsAcceptsEmptyArrayValue() {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("teams", new ArrayList<>());
        SetRLSPrincipalTagsReq request = SetRLSPrincipalTagsReq.builder()
                .databaseName("default")
                .collectionName("test")
                .principalName("alice")
                .tags(tags)
                .build();
        client_v2.setRLSPrincipalTags(request);

        ArgumentCaptor<SetRLSPrincipalTagsRequest> captor = ArgumentCaptor.forClass(SetRLSPrincipalTagsRequest.class);
        verify(blockingStub).setRLSPrincipalTags(captor.capture());
        assertEquals("alice", captor.getValue().getPrincipalName());
        assertEquals("{\"teams\":[]}", captor.getValue().getTags());
    }
}
