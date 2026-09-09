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

package io.milvus.unit.v2.utils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.ClientUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class ClientUtilsTest {
    private final ClientUtils clientUtils = new ClientUtils();

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void configureProxyRegistersDetectorForHostPort() {
        ManagedChannelBuilder builder = mock(ManagedChannelBuilder.class);

        ClientUtils.configureProxy(builder, "proxy.example:8080");

        verify(builder).proxyDetector(any());
    }

    @Test
    void configureProxyIgnoresMalformedAddresses() {
        ManagedChannelBuilder builder = mock(ManagedChannelBuilder.class);

        ClientUtils.configureProxy(builder, "proxy.example:8080:extra");
        ClientUtils.configureProxy(builder, "no-port");
        ClientUtils.configureProxy(builder, "");

        verify(builder, never()).proxyDetector(any());
    }

    @Test
    void checkDatabaseExistPassesWhenDatabaseListed() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.listDatabases(any())).thenReturn(ListDatabasesResponse.newBuilder()
                .setStatus(success())
                .addAllDbNames(Arrays.asList("default", "db1"))
                .build());

        assertDoesNotThrow(() -> clientUtils.checkDatabaseExist(stub, "db1"));
        verify(stub).listDatabases(any(ListDatabasesRequest.class));
    }

    @Test
    void checkDatabaseExistThrowsWhenDatabaseMissing() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.listDatabases(any())).thenReturn(ListDatabasesResponse.newBuilder()
                .setStatus(success())
                .addAllDbNames(Collections.singletonList("default"))
                .build());

        assertThrows(IllegalArgumentException.class, () -> clientUtils.checkDatabaseExist(stub, "missing"));
    }

    @Test
    void validateHostnameAcceptsResolvableHost() {
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();

        assertDoesNotThrow(() -> clientUtils.validateHostname(config));
    }

    @Test
    void validateHostnameRejectsEmptyHost() {
        ConnectConfig config = mock(ConnectConfig.class);
        when(config.getHost()).thenReturn("");

        assertThrows(MilvusClientException.class, () -> clientUtils.validateHostname(config));
    }

    @Test
    void validateHostnameRejectsUnresolvableHost() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://nonexistent-host.invalid:19530")
                .build();

        assertThrows(MilvusClientException.class, () -> clientUtils.validateHostname(config));
    }

    @Test
    void validatePortRejectsOutOfRangePort() {
        ConnectConfig config = mock(ConnectConfig.class);
        when(config.getPort()).thenReturn(0);
        when(config.getHost()).thenReturn("localhost");

        assertThrows(MilvusClientException.class, () -> clientUtils.validatePort(config));
    }

    @Test
    void validatePortRejectsUnreachablePort() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://127.0.0.1:1")
                .connectTimeoutMs(1000)
                .build();

        assertThrows(MilvusClientException.class, () -> clientUtils.validatePort(config));
    }

    @Test
    void validateCertSkipsWhenNotSecure() {
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();

        assertDoesNotThrow(() -> clientUtils.validateCert(config));
    }

    @Test
    void validateCertThrowsOnUnreadableCertFile() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .secure(true)
                .serverPemPath("does-not-exist.pem")
                .build();

        assertThrows(MilvusClientException.class, () -> clientUtils.validateCert(config));
    }

    @Test
    void getChannelBuildsPlaintextChannel() {
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();

        ManagedChannel channel = clientUtils.getChannel(config);

        assertNotNull(channel);
    }

    @Test
    void getChannelBuildsPlaintextChannelWithoutInterceptor() {
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();

        ManagedChannel channel = clientUtils.getChannel(config, null);

        assertNotNull(channel);
    }

    @Test
    void getHostNameReturnsNonEmptyString() {
        assertNotNull(clientUtils.getHostName());
    }

    @Test
    void getLocalTimeStrReturnsNonEmptyString() {
        assertTrue(clientUtils.getLocalTimeStr().length() > 0);
    }

    @Test
    void getSDKVersionReturnsString() {
        assertNotNull(clientUtils.getSDKVersion());
    }
}
