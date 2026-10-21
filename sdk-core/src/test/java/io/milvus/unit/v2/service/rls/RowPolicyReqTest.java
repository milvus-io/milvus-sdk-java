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

package io.milvus.unit.v2.service.rls;

import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.common.RowPolicyType;
import io.milvus.v2.service.rls.request.CreateRowPolicyReq;
import io.milvus.v2.service.rls.request.DeleteRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.DropRowPolicyReq;
import io.milvus.v2.service.rls.request.GetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.ListRLSPrincipalsReq;
import io.milvus.v2.service.rls.request.ListRowPoliciesReq;
import io.milvus.v2.service.rls.request.SetRLSPrincipalTagsReq;
import io.milvus.v2.service.rls.request.UpdateRowPolicyReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class RowPolicyReqTest {

    private static CreateRowPolicyReq minimalCreate() {
        return CreateRowPolicyReq.builder()
                .collectionName("test")
                .policyName("policy_test")
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build();
    }

    @Test
    void createRowPolicyBuilderBuildsAllFields() {
        CreateRowPolicyReq request = CreateRowPolicyReq.builder()
                .databaseName("default")
                .collectionName("test")
                .policyName("policy_test")
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY, RowPolicyAction.SEARCH))
                .usingExpr("user_id == 1")
                .checkExpr("user_id == 1")
                .description("test policy")
                .build();

        assertEquals("default", request.getDatabaseName());
        assertEquals("test", request.getCollectionName());
        assertEquals("policy_test", request.getPolicyName());
        assertEquals(RowPolicyType.PERMISSIVE, request.getPolicyType());
        assertEquals(2, request.getActions().size());
        assertEquals("user_id == 1", request.getUsingExpr());
        assertEquals("user_id == 1", request.getCheckExpr());
        assertEquals("test policy", request.getDescription());
    }

    @Test
    void createRowPolicyRequiresPolicyType() {
        assertThrows(IllegalArgumentException.class, () -> CreateRowPolicyReq.builder()
                .collectionName("test")
                .policyName("policy_test")
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build());
        assertThrows(IllegalArgumentException.class, () -> CreateRowPolicyReq.builder()
                .policyName("policy_test")
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build());
        assertThrows(IllegalArgumentException.class, () -> CreateRowPolicyReq.builder()
                .collectionName("")
                .policyName("policy_test")
                .policyType(RowPolicyType.PERMISSIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build());
    }

    @Test
    void createRowPolicyRequiresActions() {
        assertThrows(IllegalArgumentException.class, () -> CreateRowPolicyReq.builder()
                .collectionName("test")
                .policyName("policy_test")
                .policyType(RowPolicyType.PERMISSIVE)
                .build());
    }

    @Test
    void createRowPolicySettersUpdateFields() {
        CreateRowPolicyReq request = minimalCreate();

        request.setPolicyName("admin");
        request.setPolicyType(RowPolicyType.RESTRICTIVE);

        assertEquals("admin", request.getPolicyName());
        assertEquals(RowPolicyType.RESTRICTIVE, request.getPolicyType());
    }

    @Test
    void updateRowPolicyBuilderBuildsAllFields() {
        UpdateRowPolicyReq request = UpdateRowPolicyReq.builder()
                .databaseName("default")
                .collectionName("test")
                .policyName("policy_test")
                .policyType(RowPolicyType.RESTRICTIVE)
                .actions(Arrays.asList(RowPolicyAction.INSERT))
                .usingExpr("team == \"a\"")
                .build();

        assertEquals("default", request.getDatabaseName());
        assertEquals("test", request.getCollectionName());
        assertEquals("policy_test", request.getPolicyName());
        assertEquals(RowPolicyType.RESTRICTIVE, request.getPolicyType());
        assertEquals(1, request.getActions().size());
        assertEquals(RowPolicyAction.INSERT, request.getActions().get(0));
        assertEquals("team == \"a\"", request.getUsingExpr());
    }

    @Test
    void updateRowPolicyRequiresPolicyTypeAndActions() {
        assertThrows(IllegalArgumentException.class, () -> UpdateRowPolicyReq.builder()
                .collectionName("test")
                .policyName("policy_test")
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build());
        assertThrows(IllegalArgumentException.class, () -> UpdateRowPolicyReq.builder()
                .collectionName("test")
                .policyName("policy_test")
                .policyType(RowPolicyType.RESTRICTIVE)
                .build());
        assertThrows(IllegalArgumentException.class, () -> UpdateRowPolicyReq.builder()
                .collectionName("")
                .policyName("policy_test")
                .policyType(RowPolicyType.RESTRICTIVE)
                .actions(Arrays.asList(RowPolicyAction.QUERY))
                .build());
    }

    @Test
    void otherRlsRequestsRequireRequiredFields() {
        // drop policy: collection + policy name required
        assertThrows(IllegalArgumentException.class, () -> DropRowPolicyReq.builder().policyName("p").build());
        assertThrows(IllegalArgumentException.class, () -> DropRowPolicyReq.builder().collectionName("c").build());
        assertThrows(IllegalArgumentException.class, () -> DropRowPolicyReq.builder().collectionName("c").policyName("").build());

        // principal tag requests: collection + principal name required
        assertThrows(IllegalArgumentException.class, () -> SetRLSPrincipalTagsReq.builder().principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> SetRLSPrincipalTagsReq.builder().collectionName("c").build());
        assertThrows(IllegalArgumentException.class, () -> SetRLSPrincipalTagsReq.builder().collectionName("").principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> GetRLSPrincipalTagsReq.builder().principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> GetRLSPrincipalTagsReq.builder().collectionName("c").build());
        assertThrows(IllegalArgumentException.class, () -> GetRLSPrincipalTagsReq.builder().collectionName("").principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> DeleteRLSPrincipalTagsReq.builder().principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> DeleteRLSPrincipalTagsReq.builder().collectionName("c").build());
        assertThrows(IllegalArgumentException.class, () -> DeleteRLSPrincipalTagsReq.builder().collectionName("").principalName("alice").build());
        assertThrows(IllegalArgumentException.class, () -> SetRLSPrincipalTagsReq.builder().collectionName("c").principalName("").build());

        // list requests: collection name required
        assertThrows(IllegalArgumentException.class, () -> ListRowPoliciesReq.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ListRowPoliciesReq.builder().collectionName("").build());
        assertThrows(IllegalArgumentException.class, () -> ListRLSPrincipalsReq.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ListRLSPrincipalsReq.builder().collectionName("").build());
    }
}
