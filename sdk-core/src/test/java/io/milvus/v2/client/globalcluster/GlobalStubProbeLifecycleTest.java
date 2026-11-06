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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.v2.client.globalcluster;

import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Tag("unit")
class GlobalStubProbeLifecycleTest {

    @Test
    void initialWaitsForActivationAndPreservesConstructorTelemetrySemantics() throws Exception {
        GlobalTopology first = topology(1, "host-one:19530");
        GlobalTopology newer = topology(2, "host-two:19530");
        MilvusClientV2 initialClient = mock(MilvusClientV2.class);
        MilvusClientV2 replacementClient = mock(MilvusClientV2.class);
        ClientTelemetryManager.RuntimeState runtimeState =
                mock(ClientTelemetryManager.RuntimeState.class);
        ConnectConfig config = ConnectConfig.builder()
                .uri("https://host.global-cluster.example")
                .deferTelemetryStart(true)
                .build();
        config.setTelemetryRuntimeState(runtimeState);
        AtomicReference<Predicate<GlobalTopology>> watcher = new AtomicReference<>();
        AtomicReference<ClientTelemetryManager.RuntimeState> initialState = new AtomicReference<>();
        AtomicReference<Boolean> initialDefer = new AtomicReference<>();
        List<MilvusClientV2> published = new ArrayList<>();
        GlobalStub.ClientFactory clientFactory = (endpoint, state, deferTelemetryStart) -> {
            if (initialState.compareAndSet(null, state)) {
                initialDefer.set(deferTelemetryStart);
                return initialClient;
            }
            return replacementClient;
        };
        TopologyFetcher fetcher = (endpoint, token, cachedVersion, callback) -> {
            watcher.set(callback);
            return first;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalStub stub = new GlobalStub("https://host.global-cluster.example", config,
                published::add, fetcher, clientFactory);
        try {
            assertSame(runtimeState, initialState.get());
            assertTrue(initialDefer.get());
            assertNull(config.takeTelemetryRuntimeState());
            assertSame(first, stub.getTopology());
            assertSame(initialClient, stub.getPrimaryClient());

            Future<?> delivery = executor.submit(() -> watcher.get().test(newer));
            assertThrows(TimeoutException.class, () -> delivery.get(200, TimeUnit.MILLISECONDS));
            assertSame(first, stub.getTopology());
            assertSame(initialClient, stub.getPrimaryClient());
            assertTrue(published.isEmpty());

            stub.activate();
            stub.activate();
            delivery.get(2, TimeUnit.SECONDS);

            assertSame(newer, stub.getTopology());
            assertSame(replacementClient, stub.getPrimaryClient());
            assertEquals(Collections.singletonList(replacementClient), published);
            verify(initialClient).close();
            verify(replacementClient).startTelemetry();
        } finally {
            stub.close();
            executor.shutdownNow();
        }
    }

    @Test
    void constructorFailureClosesInitialCallbackGate() {
        GlobalTopology newer = topology(2, "host-two:19530");
        ConnectConfig config = ConnectConfig.builder()
                .uri("https://host.global-cluster.example")
                .build();
        AtomicReference<Predicate<GlobalTopology>> watcher = new AtomicReference<>();
        TopologyFetcher fetcher = (endpoint, token, cachedVersion, callback) -> {
            watcher.set(callback);
            throw new IllegalStateException("discovery failed");
        };

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new GlobalStub("https://host.global-cluster.example", config,
                        client -> { }, fetcher,
                        (endpoint, state, deferred) -> mock(MilvusClientV2.class)));

        assertEquals("discovery failed", exception.getMessage());
        assertFalse(watcher.get().test(newer));
    }

    @Test
    void closeBeforeActivationReleasesWatcherWithoutReplacingClient() throws Exception {
        GlobalTopology first = topology(1, "host-one:19530");
        GlobalTopology newer = topology(2, "host-two:19530");
        MilvusClientV2 initialClient = mock(MilvusClientV2.class);
        ConnectConfig config = ConnectConfig.builder()
                .uri("https://host.global-cluster.example")
                .build();
        AtomicReference<Predicate<GlobalTopology>> watcher = new AtomicReference<>();
        AtomicInteger creations = new AtomicInteger();
        List<MilvusClientV2> published = new ArrayList<>();
        TopologyFetcher fetcher = (endpoint, token, cachedVersion, callback) -> {
            watcher.set(callback);
            return first;
        };
        GlobalStub.ClientFactory clientFactory = (endpoint, state, deferTelemetryStart) -> {
            creations.incrementAndGet();
            return initialClient;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalStub stub = new GlobalStub("https://host.global-cluster.example", config,
                published::add, fetcher, clientFactory);
        try {
            Future<?> delivery = executor.submit(() -> watcher.get().test(newer));
            assertThrows(TimeoutException.class, () -> delivery.get(200, TimeUnit.MILLISECONDS));

            stub.close();
            delivery.get(2, TimeUnit.SECONDS);
            stub.activate();

            assertSame(first, stub.getTopology());
            assertNull(stub.getPrimaryClient());
            assertEquals(1, creations.get());
            assertTrue(published.isEmpty());
            verify(initialClient).close();
        } finally {
            stub.close();
            executor.shutdownNow();
        }
    }

    private static GlobalTopology topology(long version, String endpoint) {
        return new GlobalTopology(version, Collections.singletonList(
                new ClusterInfo("cluster", endpoint, ClusterCapability.WRITABLE)));
    }
}
