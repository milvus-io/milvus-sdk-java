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

package io.milvus.v2.client.globalcluster;

import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.telemetry.TelemetryConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
class GlobalStubTelemetryHandoffTest {
    @Test
    void retargetPublishesCurrentAndFuturePrimaryToNewOwner() {
        ClientTelemetryManager oldManager = manager("");
        oldManager.start();
        TestMilvusClient oldClient = new TestMilvusClient(oldManager);
        ClientTelemetryManager newManager = manager(oldManager.getClientId());
        TestMilvusClient newClient = new TestMilvusClient(newManager);
        GlobalStub stub = testStub(oldClient, (endpoint, state, deferred) -> newClient);
        AtomicReference<MilvusClientV2> published = new AtomicReference<>();

        try {
            stub.retargetPrimaryChange(published::set);
            assertSame(oldClient, published.get());

            stub.onTopologyChange(topology(2, "new-primary:19530"));
            assertSame(newClient, published.get());
        } finally {
            newManager.close();
            oldManager.close();
        }
    }

    @Test
    void replacementConstructionFailureKeepsOldTelemetryRunning() {
        ClientTelemetryManager oldManager = manager("");
        oldManager.start();
        TestMilvusClient oldClient = new TestMilvusClient(oldManager);
        GlobalStub stub = testStub(oldClient, (endpoint, state, deferred) -> {
            throw new IllegalStateException("connect failed");
        });

        try {
            assertThrows(IllegalStateException.class,
                    () -> stub.onTopologyChange(topology(2, "new-primary:19530")));
            assertSame(oldClient, stub.getPrimaryClient());
            assertEquals("old-primary:19530", stub.getPrimaryEndpoint());
            assertTrue(oldManager.isReady());
            assertFalse(oldManager.isClosed());
            assertFalse(oldClient.closed);
        } finally {
            oldManager.close();
        }
    }

    @Test
    void successfulReplacementPreservesStateClosesOldAndForwardsLateOperations() throws Exception {
        ClientTelemetryManager oldManager = manager("");
        oldManager.recordOperation("Search", "books", System.nanoTime(), "", "");
        oldManager.start();
        TestMilvusClient oldClient = new TestMilvusClient(oldManager);

        ClientTelemetryManager newManager = manager(oldManager.getClientId());
        TestMilvusClient newClient = new TestMilvusClient(newManager);
        AtomicBoolean deferred = new AtomicBoolean();
        GlobalStub stub = testStub(oldClient, (endpoint, state, deferTelemetryStart) -> {
            assertEquals(oldManager.getClientId(), state.getClientId());
            deferred.set(deferTelemetryStart);
            return newClient;
        });

        try {
            stub.onTopologyChange(topology(2, "new-primary:19530"));
            assertTrue(deferred.get());
            assertSame(newClient, stub.getPrimaryClient());
            assertEquals("new-primary:19530", stub.getPrimaryEndpoint());
            assertTrue(oldManager.isClosed());
            assertTrue(newManager.isReady());
            assertEquals(oldManager.getClientId(), newManager.getClientId());
            assertTrue(oldClient.closed);

            // A foreground request that captured the old manager before the switch must not vanish.
            oldManager.recordOperation("Search", "books", System.nanoTime(), "", "");
            invokeCreateSnapshot(newManager);
            assertEquals(2, awaitRequestCount(newManager, "Search"));
        } finally {
            newManager.close();
        }
    }

    @Test
    void outerStubSwitchFailureRollsBackWithoutStoppingOldTelemetry() {
        ClientTelemetryManager oldManager = manager("");
        oldManager.start();
        TestMilvusClient oldClient = new TestMilvusClient(oldManager);
        ClientTelemetryManager newManager = manager(oldManager.getClientId());
        TestMilvusClient newClient = new TestMilvusClient(newManager);
        AtomicInteger callbacks = new AtomicInteger();
        ConnectConfig config = ConnectConfig.builder().uri("http://global:19530").build();
        GlobalStub stub = new GlobalStub("http://global:19530", config, client -> {
            callbacks.incrementAndGet();
            if (client == newClient) {
                throw new IllegalStateException("outer switch failed");
            }
        }, topology(1, "old-primary:19530"), oldClient,
                (endpoint, state, deferred) -> newClient);

        try {
            assertThrows(IllegalStateException.class,
                    () -> stub.onTopologyChange(topology(2, "new-primary:19530")));
            assertSame(oldClient, stub.getPrimaryClient());
            assertEquals("old-primary:19530", stub.getPrimaryEndpoint());
            assertEquals(2, callbacks.get());
            assertFalse(oldManager.isClosed());
            assertTrue(oldManager.isReady());
            assertTrue(newManager.isClosed());
            assertFalse(oldClient.closed);
            assertTrue(newClient.closed);
            // The failed outer switch must have cancelled retirement, so the old manager can
            // enter and leave a later retirement normally instead of remaining fenced forever.
            long retirementToken = oldManager.beginRuntimeStateRetirement();
            oldManager.cancelRuntimeStateRetirement(retirementToken);
        } finally {
            oldManager.close();
        }
    }

    private static GlobalStub testStub(MilvusClientV2 oldClient, GlobalStub.ClientFactory factory) {
        ConnectConfig config = ConnectConfig.builder().uri("http://global:19530").build();
        return new GlobalStub("http://global:19530", config, ignored -> { },
                topology(1, "old-primary:19530"), oldClient, factory);
    }

    private static GlobalTopology topology(long version, String endpoint) {
        return new GlobalTopology(version, Collections.singletonList(
                new ClusterInfo("cluster", endpoint, ClusterCapability.WRITABLE)));
    }

    private static ClientTelemetryManager manager(String clientId) {
        return new ClientTelemetryManager(TelemetryConfig.defaults(), "", "test",
                () -> "default", null, clientId);
    }

    private static void invokeCreateSnapshot(ClientTelemetryManager manager) throws Exception {
        Method method = ClientTelemetryManager.class.getDeclaredMethod("createSnapshot");
        method.setAccessible(true);
        method.invoke(manager);
    }

    private static long requestCount(ClientTelemetryManager manager, String operationName) {
        long count = 0;
        for (ClientTelemetryManager.MetricsSnapshot snapshot : manager.getMetricsSnapshots()) {
            for (ClientTelemetryManager.OperationSnapshot operation : snapshot.metrics) {
                if (operationName.equals(operation.operation)) {
                    count += operation.global.request_count;
                }
            }
        }
        return count;
    }

    private static long awaitRequestCount(
            ClientTelemetryManager manager, String operationName) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        long count;
        do {
            count = requestCount(manager, operationName);
            if (count >= 2) {
                return count;
            }
            Thread.sleep(1);
        } while (System.nanoTime() < deadline);
        return count;
    }

    private static final class TestMilvusClient extends MilvusClientV2 {
        private final ClientTelemetryManager manager;
        private boolean closed;

        private TestMilvusClient(ClientTelemetryManager manager) {
            super(null);
            this.manager = manager;
        }

        @Override
        public ClientTelemetryManager getTelemetry() {
            return manager;
        }

        @Override
        public void startTelemetry() {
            manager.start();
        }

        @Override
        public void close() {
            closed = true;
            manager.close();
        }
    }
}
