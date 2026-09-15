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

package io.milvus.telemetry;

import com.google.protobuf.ByteString;
import io.milvus.grpc.ClientCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetrySnapshotSwapTest {

    @Test
    void sequentialSnapshotsReportSeparateWindows() throws Exception {
        ClientTelemetryManager manager = newManager();
        try {
            manager.recordOperation("Search", "books",
                    System.nanoTime() - TimeUnit.MICROSECONDS.toNanos(3), "", "");
            invoke(manager, "createSnapshot");
            manager.recordOperation("Search", "books",
                    System.nanoTime() - TimeUnit.MICROSECONDS.toNanos(4), "", "");
            invoke(manager, "createSnapshot");

            List<ClientTelemetryManager.MetricsSnapshot> snapshots = manager.getMetricsSnapshots();
            assertEquals(2, snapshots.size());
            assertEquals(1, snapshots.get(0).metrics.get(0).global.request_count);
            assertEquals(1, snapshots.get(1).metrics.get(0).global.request_count);
            // The fresh bucket is the next window: the boundary captured at the swap is
            // reused as both the current window's end and the next window's start.
            assertEquals(snapshots.get(0).end_time, snapshots.get(1).timestamp);
        } finally {
            manager.close();
        }
    }

    @Test
    void recordsDuringSnapshotSwapStartFreshWindow() throws Exception {
        ClientTelemetryManager manager = newManager();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // Many operations with full buckets so the snapshot loop runs long enough
            // to observe the O(1) swap from the test thread.
            String[] operations = new String[2000];
            for (int index = 0; index < operations.length; index++) {
                operations[index] = "op_" + index;
            }
            populate(manager, operations, 500);

            Field collectorsField = ClientTelemetryManager.class.getDeclaredField("collectors");
            collectorsField.setAccessible(true);
            Object collectorsLock = getField(manager, "collectorsLock");
            Map<?, ?> original;
            synchronized (collectorsLock) {
                original = (Map<?, ?>) collectorsField.get(manager);
            }
            assertNotNull(original);

            Future<?> snapshot = pool.submit(() -> {
                invoke(manager, "createSnapshot");
                return null;
            });

            // Wait until the swap replaced the collectors reference. Reading under the
            // collectors lock gives a proper happens-before edge with the swap write.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            boolean swapped = false;
            while (System.nanoTime() < deadline) {
                Map<?, ?> current;
                synchronized (collectorsLock) {
                    current = (Map<?, ?>) collectorsField.get(manager);
                }
                if (current != original) {
                    swapped = true;
                    break;
                }
                Thread.yield();
            }
            assertTrue(swapped, "snapshot swap was not observed");

            // The swap has committed: this record lands in the fresh bucket.
            manager.recordOperation("concurrent-op", "collection_0",
                    System.nanoTime() - TimeUnit.MICROSECONDS.toNanos(5), "", "");
            snapshot.get(10, TimeUnit.SECONDS);

            // The in-flight snapshot was built from the old bucket only.
            List<ClientTelemetryManager.MetricsSnapshot> snapshots = manager.getMetricsSnapshots();
            assertFalse(snapshots.isEmpty());
            ClientTelemetryManager.MetricsSnapshot latest = snapshots.get(snapshots.size() - 1);
            assertFalse(containsOperation(latest, "concurrent-op"));

            // Draining the fresh bucket reports the concurrent record in the next window.
            invoke(manager, "createSnapshot");
            List<ClientTelemetryManager.MetricsSnapshot> after = manager.getMetricsSnapshots();
            ClientTelemetryManager.MetricsSnapshot next = after.get(after.size() - 1);
            assertTrue(containsOperation(next, "concurrent-op"));
        } finally {
            pool.shutdownNow();
            manager.close();
        }
    }

    private static ClientTelemetryManager newManager() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        manager.processCommands(Collections.singletonList(ClientCommand.newBuilder()
                .setCommandId("enable-all")
                .setCommandType("collection_metrics")
                .setPayload(ByteString.copyFromUtf8("{\"enabled\":true,\"collections\":[\"*\"]}"))
                .setCreateTime(1)
                .build()));
        return manager;
    }

    private static void populate(ClientTelemetryManager manager, String[] operations,
                                 int recordsPerOperation) {
        for (String operation : operations) {
            for (int record = 0; record < recordsPerOperation; record++) {
                manager.recordOperation(operation, "collection_0",
                        System.nanoTime() - TimeUnit.MICROSECONDS.toNanos(record % 1000 + 1), "", "");
            }
        }
    }

    private static boolean containsOperation(
            ClientTelemetryManager.MetricsSnapshot snapshot, String operation) {
        for (ClientTelemetryManager.OperationSnapshot item : snapshot.metrics) {
            if (item.operation.equals(operation)) {
                return true;
            }
        }
        return false;
    }

    private static void invoke(Object target, String name) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        method.invoke(target);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
