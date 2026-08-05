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

package io.milvus.param;

import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.milvus.common.utils.RedactCredential.redactUriUserInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialRedactionTest {
    @Test
    void globalClusterEndpointLoggingRedactsUriUserInfo() {
        String loggedEndpoint = redactUriUserInfo(
                "https://uri-user:uri-password@foo.global-cluster.example");

        assertFalse(loggedEndpoint.contains("uri-user"));
        assertFalse(loggedEndpoint.contains("uri-password"));
        assertEquals("https://<redacted>@foo.global-cluster.example", loggedEndpoint);
    }

    @Test
    void uriRedactionUsesLastAtSignWithinAuthority() {
        assertEquals("https://<redacted>@host:19530/default",
                redactUriUserInfo("https://user:p@ss@host:19530/default"));
    }

    @Test
    void uriRedactionIgnoresQueryAndFragmentAtSigns() {
        String queryUri = "https://host:19530/default?owner=a@b";
        String fragmentUri = "https://host:19530/default#owner=a@b";

        assertEquals(queryUri, redactUriUserInfo(queryUri));
        assertEquals(fragmentUri, redactUriUserInfo(fragmentUri));
    }

    @Test
    void connectParamToStringRedactsCredentials() {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri("http://uri-user:uri-password@localhost:19530/default")
                .withToken("sensitive-token")
                .withAuthorization("sensitive-user", "sensitive-password")
                .build();

        assertCredentialsRedacted(connectParam.toString(),
                "sensitive-token", "sensitive-password",
                "uri-user", "uri-password");
        assertTrue(connectParam.toString().contains(
                "uri='http://<redacted>@localhost:19530/default'"));
        assertTrue(connectParam.toString().contains("token='<redacted>'"));
        assertTrue(connectParam.toString().contains("authorization='<redacted>'"));
        assertTrue(connectParam.toString().contains("userName='sensitive-user'"));
    }

    @Test
    void multiConnectParamToStringRedactsInheritedCredentials() {
        MultiConnectParam connectParam = MultiConnectParam.newBuilder()
                .withHosts(Collections.singletonList(ServerAddress.newBuilder().build()))
                .withToken("sensitive-token")
                .withAuthorization("sensitive-user", "sensitive-password")
                .build();

        assertCredentialsRedacted(connectParam.toString(),
                "sensitive-token", "sensitive-password");
        assertTrue(connectParam.toString().contains("userName='sensitive-user'"));
    }

    @Test
    void credentialParamsToStringRedactsCredentials() {
        CreateCredentialParam createParam = CreateCredentialParam.newBuilder()
                .withUsername("sensitive-user")
                .withPassword("sensitive-password")
                .build();
        UpdateCredentialParam updateParam = UpdateCredentialParam.newBuilder()
                .withUsername("sensitive-user")
                .withOldPassword("sensitive-old-password")
                .withNewPassword("sensitive-new-password")
                .build();
        DeleteCredentialParam deleteParam = DeleteCredentialParam.newBuilder()
                .withUsername("sensitive-user")
                .build();

        assertCredentialsRedacted(createParam.toString(),
                "sensitive-password");
        assertCredentialsRedacted(updateParam.toString(),
                "sensitive-old-password", "sensitive-new-password");
        assertTrue(createParam.toString().contains("username='sensitive-user'"));
        assertTrue(createParam.toString().contains("password='<redacted>'"));
        assertTrue(updateParam.toString().contains("username='sensitive-user'"));
        assertTrue(updateParam.toString().contains("oldPassword='<redacted>'"));
        assertTrue(updateParam.toString().contains("newPassword='<redacted>'"));
        assertTrue(deleteParam.toString().contains("username='sensitive-user'"));
    }

    @Test
    void v2CredentialParamsToStringRedactsSecrets() {
        CreateUserReq createUserReq = CreateUserReq.builder()
                .userName("sensitive-user")
                .password("sensitive-password")
                .build();
        UpdatePasswordReq updatePasswordReq = UpdatePasswordReq.builder()
                .userName("sensitive-user")
                .password("sensitive-old-password")
                .newPassword("sensitive-new-password")
                .build();
        MilvusCluster milvusCluster = MilvusCluster.builder()
                .clusterId("cluster")
                .uri("http://uri-user:uri-password@localhost:19530")
                .token("sensitive-token")
                .build();

        assertCredentialsRedacted(createUserReq.toString(), "sensitive-password");
        assertCredentialsRedacted(updatePasswordReq.toString(),
                "sensitive-old-password", "sensitive-new-password");
        assertCredentialsRedacted(milvusCluster.toString(),
                "sensitive-token", "uri-user", "uri-password");
        assertTrue(createUserReq.toString().contains("userName='sensitive-user'"));
        assertTrue(createUserReq.toString().contains("password='<redacted>'"));
        assertTrue(updatePasswordReq.toString().contains("userName='sensitive-user'"));
        assertTrue(updatePasswordReq.toString().contains("password='<redacted>'"));
        assertTrue(updatePasswordReq.toString().contains("newPassword='<redacted>'"));
        assertTrue(milvusCluster.toString().contains(
                "uri='http://<redacted>@localhost:19530'"));
        assertTrue(milvusCluster.toString().contains("token='<redacted>'"));
    }

    private static void assertCredentialsRedacted(String output, String... credentials) {
        for (String credential : credentials) {
            assertFalse(output.contains(credential));
        }
    }
}
