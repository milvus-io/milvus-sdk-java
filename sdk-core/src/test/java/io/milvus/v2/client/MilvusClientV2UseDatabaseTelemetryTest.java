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

package io.milvus.v2.client;

import io.grpc.ManagedChannel;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.telemetry.TelemetryConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Tag("unit")
class MilvusClientV2UseDatabaseTelemetryTest {
    @Test
    void candidateFailureLeavesActiveConnectionAndTelemetryUntouched() throws Exception {
        ConnectConfig config = config("default");
        ClientTelemetryManager oldManager = manager("client-id");
        oldManager.start();
        ManagedChannel oldChannel = mock(ManagedChannel.class);
        TestClient client = new TestClient(candidateConfig -> {
            throw new IllegalStateException("candidate failed");
        });
        installConnection(client, config, oldManager, oldChannel);

        try {
            assertThrows(IllegalStateException.class, () -> client.useDatabase("other"));
            assertEquals("default", client.currentUsedDatabase());
            assertSame(oldManager, client.getTelemetry());
            assertTrue(oldManager.isReady());
            assertFalse(oldManager.isClosed());
            verify(oldChannel, never()).shutdownNow();
        } finally {
            oldManager.close();
        }
    }

    @Test
    void successfulCandidatePreservesTelemetryStateAndThenClosesOldConnection() throws Exception {
        ConnectConfig config = config("default");
        ClientTelemetryManager oldManager = manager("client-id");
        oldManager.start();
        ManagedChannel oldChannel = mock(ManagedChannel.class);
        ManagedChannel newChannel = mock(ManagedChannel.class);
        AtomicReference<ConnectConfig> seenConfig = new AtomicReference<>();
        AtomicReference<ClientTelemetryManager> newManagerRef = new AtomicReference<>();
        TestClient client = new TestClient(candidateConfig -> {
            seenConfig.set(candidateConfig);
            ClientTelemetryManager replacement = manager(candidateConfig.getTelemetryClientId());
            newManagerRef.set(replacement);
            MilvusClientV2 candidate = new MilvusClientV2(null);
            installConnection(candidate, candidateConfig, replacement, newChannel);
            return candidate;
        });
        installConnection(client, config, oldManager, oldChannel);

        try {
            client.useDatabase("other");

            ClientTelemetryManager replacement = newManagerRef.get();
            assertEquals("other", seenConfig.get().getDbName());
            assertTrue(seenConfig.get().isDeferTelemetryStart());
            assertEquals(oldManager.getClientId(), seenConfig.get().getTelemetryClientId());
            assertEquals("other", client.currentUsedDatabase());
            assertEquals("other", config.getDbName());
            assertSame(replacement, client.getTelemetry());
            assertEquals(oldManager.getClientId(), replacement.getClientId());
            assertTrue(oldManager.isClosed());
            assertTrue(replacement.isReady());
            verify(oldChannel).shutdownNow();
            verify(newChannel, never()).shutdownNow();
        } finally {
            client.close();
        }
    }

    @Test
    void failedStateHandoffRollsBackPublishedCandidateAndKeepsOldLifecycle() throws Exception {
        ConnectConfig config = config("default");
        ClientTelemetryManager oldManager = manager("client-id");
        oldManager.start();
        ManagedChannel oldChannel = mock(ManagedChannel.class);
        ManagedChannel candidateChannel = mock(ManagedChannel.class);
        AtomicReference<ClientTelemetryManager> candidateManagerRef = new AtomicReference<>();
        TestClient client = new TestClient(candidateConfig -> {
            ClientTelemetryManager candidateManager = manager("different-client-id");
            candidateManagerRef.set(candidateManager);
            MilvusClientV2 candidate = new MilvusClientV2(null);
            installConnection(candidate, candidateConfig, candidateManager, candidateChannel);
            return candidate;
        });
        installConnection(client, config, oldManager, oldChannel);

        try {
            assertThrows(IllegalArgumentException.class, () -> client.useDatabase("other"));
            assertEquals("default", client.currentUsedDatabase());
            assertSame(oldManager, client.getTelemetry());
            assertTrue(oldManager.isReady());
            assertFalse(oldManager.isClosed());
            assertTrue(candidateManagerRef.get().isClosed());
            verify(oldChannel, never()).shutdownNow();
            verify(candidateChannel).shutdownNow();
        } finally {
            oldManager.close();
        }
    }

    private static ConnectConfig config(String database) {
        return ConnectConfig.builder()
                .uri("http://localhost:19530")
                .dbName(database)
                .telemetryConfig(TelemetryConfig.builder()
                        .heartbeatIntervalMs(TimeUnit.MINUTES.toMillis(10))
                        .build())
                .telemetryClientId("client-id")
                .build();
    }

    private static ClientTelemetryManager manager(String clientId) {
        return new ClientTelemetryManager(
                TelemetryConfig.builder()
                        .heartbeatIntervalMs(TimeUnit.MINUTES.toMillis(10))
                        .build(),
                "", "test", () -> "default", null, clientId);
    }

    private static void installConnection(
            MilvusClientV2 client,
            ConnectConfig config,
            ClientTelemetryManager manager,
            ManagedChannel channel) {
        try {
            setField(client, "connectConfig", config);
            setField(client, "cacheEndpoint", "localhost:19530");
            setField(client, "telemetry", manager);
            setField(client, "channel", channel);
            setField(client, "blockingStub", mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class));
            setField(client, "futureStub", mock(MilvusServiceGrpc.MilvusServiceFutureStub.class));
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    private static void setField(Object target, String name, Object value)
            throws ReflectiveOperationException {
        Field field = MilvusClientV2.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private interface CandidateFactory {
        MilvusClientV2 create(ConnectConfig config);
    }

    private static final class TestClient extends MilvusClientV2 {
        private final CandidateFactory candidateFactory;

        private TestClient(CandidateFactory candidateFactory) {
            super(null);
            this.candidateFactory = candidateFactory;
        }

        @Override
        MilvusClientV2 createDatabaseCandidate(ConnectConfig config) {
            return candidateFactory.create(config);
        }
    }
}
