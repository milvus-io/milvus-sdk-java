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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.milvus.grpc.ClientCommand;
import io.milvus.grpc.CommandReply;
import io.milvus.grpc.OperationMetrics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryMetricsTest {
    @Test
    void collectionMetricsUsesStrictJsonTypes() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            List<String> invalidPayloads = Arrays.asList(
                    "{\"enabled\":\"true\",\"collections\":[\"books\"]}",
                    "{\"enabled\":true,\"collections\":\"books\"}",
                    "{\"enabled\":true,\"collections\":[\"books\",1]}",
                    "{\"enabled\":true,\"collections\":[\"books\"],\"metrics_types\":true}",
                    "{\"enabled\":true,\"collections\":[\"books\"],"
                            + "\"metrics_types\":[\"latency\",null]}");
            for (int index = 0; index < invalidPayloads.size(); index++) {
                assertFalse(processOne(manager, command(
                        "bad-collections-" + index, "collection_metrics",
                        invalidPayloads.get(index), index + 1)).getSuccess());
            }

            assertTrue(processOne(manager, command(
                    "null-collections", "collection_metrics",
                    "{\"enabled\":null,\"collections\":null,\"metrics_types\":null}", 10))
                    .getSuccess());
        } finally {
            manager.close();
        }
    }

    @Test
    void queryCommandsUseStrictJsonTypes() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            List<String> invalidMaxCounts = Arrays.asList("\"1\"", "1.5", "2147483648");
            for (int index = 0; index < invalidMaxCounts.size(); index++) {
                assertFalse(processOne(manager, command(
                        "bad-max-" + index, "show_errors",
                        "{\"max_count\":" + invalidMaxCounts.get(index) + "}", index + 1))
                        .getSuccess());
            }

            String start = "2026-08-23T00:00:00Z";
            String end = "2026-08-23T00:01:00Z";
            List<String> invalidHistoryPayloads = Arrays.asList(
                    "{\"start_time\":1,\"end_time\":\"" + end + "\"}",
                    "{\"start_time\":\"" + start + "\",\"end_time\":true}",
                    "{\"start_time\":\"" + start + "\",\"end_time\":\"" + end
                            + "\",\"detail\":\"true\"}");
            for (int index = 0; index < invalidHistoryPayloads.size(); index++) {
                assertFalse(processOne(manager, command(
                        "bad-history-" + index, "show_latency_history",
                        invalidHistoryPayloads.get(index), 10 + index)).getSuccess());
            }

            assertTrue(processOne(manager, command(
                    "null-query-values", "show_errors", "{\"max_count\":null}", 20))
                    .getSuccess());
            assertTrue(processOne(manager, command(
                    "null-detail", "show_latency_history",
                    "{\"start_time\":\"" + start + "\",\"end_time\":\"" + end
                            + "\",\"detail\":null}", 21)).getSuccess());
        } finally {
            manager.close();
        }
    }

    @Test
    void latencyHistoryRequiresRfc3339SecondsAndTimezone() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            List<String> invalidPayloads = Arrays.asList(
                    "{\"start_time\":\"2026-08-23T12:34Z\","
                            + "\"end_time\":\"2026-08-23T12:35:00Z\"}",
                    "{\"start_time\":\"2026-08-23T12:34:00\","
                            + "\"end_time\":\"2026-08-23T12:35:00Z\"}",
                    "{\"start_time\":\"2026-08-23\","
                            + "\"end_time\":\"2026-08-23T12:35:00Z\"}");
            for (int index = 0; index < invalidPayloads.size(); index++) {
                assertFalse(processOne(manager, command(
                        "bad-rfc3339-" + index, "show_latency_history",
                        invalidPayloads.get(index), index + 1)).getSuccess());
            }

            assertTrue(processOne(manager, command(
                    "offset-history", "show_latency_history",
                    "{\"start_time\":\"2026-08-23T12:34:00.123456789123+08:00\","
                            + "\"end_time\":\"2026-08-23T12:35:00.987654321987+08:00\"}",
                    10)).getSuccess());
            assertTrue(processOne(manager, command(
                    "zulu-history", "show_latency_history",
                    "{\"start_time\":\"2026-08-23T04:34:00.1Z\","
                            + "\"end_time\":\"2026-08-23T04:35:00.2Z\"}",
                    11)).getSuccess());
        } finally {
            manager.close();
        }
    }

    @Test
    void commandQueryRepliesCoverStateRedactionAggregateAndRanges() throws Exception {
        Map<String, Object> supplied = new HashMap<>();
        supplied.put("address", "localhost:19530");
        supplied.put("password", "secret");
        supplied.put("token", "secret-token");
        supplied.put("api_key", "secret-key");
        supplied.put("authorization", "secret-auth");
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "db", () -> supplied);
        try {
            assertFalse(processOne(manager, command(
                    "empty-enable", "collection_metrics", "{\"enabled\":true}", 1)).getSuccess());
            assertTrue(processOne(manager, command(
                    "wildcard", "collection_metrics",
                    "{\"enabled\":true,\"collections\":[\"*\"]}", 2)).getSuccess());
            assertTrue(processOne(manager, command(
                    "named", "collection_metrics",
                    "{\"enabled\":true,\"collections\":[\"books\"]}", 3)).getSuccess());
            CommandReply collectionState = processOne(manager, command(
                    "collection-state", "collection_metrics", "", 4));
            JsonObject collectionBody = JsonParser.parseString(
                    collectionState.getPayload().toStringUtf8()).getAsJsonObject();
            assertTrue(collectionBody.get("all_collections_enabled").getAsBoolean());

            CommandReply configReply = processOne(manager, command(
                    "config-state", "get_config", "", 5));
            JsonObject userConfig = JsonParser.parseString(configReply.getPayload().toStringUtf8())
                    .getAsJsonObject().getAsJsonObject("user_config");
            assertEquals("localhost:19530", userConfig.get("address").getAsString());
            assertFalse(userConfig.has("password"));
            assertFalse(userConfig.has("token"));
            assertFalse(userConfig.has("api_key"));
            assertFalse(userConfig.has("authorization"));
            assertEquals("[\"*\"]", userConfig.getAsJsonArray("enabled_collections").toString());

            manager.recordOperation(
                    "Search", "books", System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(3), "", "");
            invoke(manager, "createSnapshot");
            ClientTelemetryManager.MetricsSnapshot snapshot = manager.getMetricsSnapshots().get(0);
            String start = java.time.Instant.ofEpochMilli(snapshot.timestamp - 1).toString();
            String end = java.time.Instant.ofEpochMilli(snapshot.end_time + 1).toString();
            CommandReply aggregate = processOne(manager, command(
                    "aggregate", "show_latency_history",
                    String.format("{\"start_time\":\"%s\",\"end_time\":\"%s\"}", start, end), 6));
            JsonObject aggregateBody = JsonParser.parseString(
                    aggregate.getPayload().toStringUtf8()).getAsJsonObject();
            assertEquals(1, aggregateBody.get("snapshot_count").getAsInt());
            assertEquals(1, aggregateBody.getAsJsonObject("aggregated")
                    .getAsJsonObject("metrics").getAsJsonObject("Search")
                    .get("request_count").getAsInt());

            assertFalse(processOne(manager, command(
                    "missing-range", "show_latency_history", "{}", 6)).getSuccess());
            assertFalse(processOne(manager, command(
                    "reverse-range", "show_latency_history",
                    "{\"start_time\":\"2026-08-23T01:00:00Z\","
                            + "\"end_time\":\"2026-08-23T00:00:00Z\"}", 8)).getSuccess());
            assertFalse(processOne(manager, command(
                    "long-range", "show_latency_history",
                    "{\"start_time\":\"2026-08-23T00:00:00Z\","
                            + "\"end_time\":\"2026-08-23T02:00:00Z\"}", 9)).getSuccess());

            assertTrue(processOne(manager, command(
                    "disable-all", "collection_metrics",
                    "{\"enabled\":false,\"collections\":[\"*\"]}", 10)).getSuccess());
        } finally {
            manager.close();
        }
    }

    @Test
    void snapshotHistoryUsesOneHourTtlWithIndependentHardCap() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            @SuppressWarnings("unchecked")
            Deque<ClientTelemetryManager.MetricsSnapshot> snapshots =
                    (Deque<ClientTelemetryManager.MetricsSnapshot>) getField(manager, "snapshots");
            long now = System.currentTimeMillis();
            synchronized (snapshots) {
                snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                        now - TimeUnit.HOURS.toMillis(1) - 2,
                        now - TimeUnit.HOURS.toMillis(1) - 1,
                        Collections.emptyList()));
                snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                        now - TimeUnit.HOURS.toMillis(1) + 1000,
                        now - TimeUnit.HOURS.toMillis(1) + 1000,
                        Collections.emptyList()));
                snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                        now - TimeUnit.MINUTES.toMillis(30),
                        now - TimeUnit.MINUTES.toMillis(30),
                        Collections.emptyList()));
            }

            manager.getConfig().setHeartbeatIntervalMs(600_000);
            List<ClientTelemetryManager.MetricsSnapshot> retained = manager.getMetricsSnapshots();
            assertEquals(2, retained.size());
            assertEquals(now - TimeUnit.HOURS.toMillis(1) + 1000, retained.get(0).end_time);

            synchronized (snapshots) {
                snapshots.clear();
                for (int index = 0; index < 4097; index++) {
                    snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                            now + index, now + index, Collections.emptyList()));
                }
            }
            retained = manager.getMetricsSnapshots();
            assertEquals(4096, retained.size());
            assertEquals(now + 1, retained.get(0).timestamp);
        } finally {
            manager.close();
        }
    }

    @Test
    void latencyDetailUsesOperationKeyedSnakeCaseMetrics() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            manager.recordOperation(
                    "Search", "books", System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(5), "", "");
            java.lang.reflect.Method createSnapshot = ClientTelemetryManager.class
                    .getDeclaredMethod("createSnapshot");
            createSnapshot.setAccessible(true);
            createSnapshot.invoke(manager);
            ClientTelemetryManager.MetricsSnapshot snapshot = manager.getMetricsSnapshots().get(0);
            java.lang.reflect.Method latencyHistory = ClientTelemetryManager.class
                    .getDeclaredMethod("handleLatencyHistory", ClientCommand.class);
            latencyHistory.setAccessible(true);

            String payload = String.format(
                    "{\"start_time\":\"%s\",\"end_time\":\"%s\",\"detail\":true}",
                    java.time.Instant.ofEpochMilli(snapshot.timestamp - 1),
                    java.time.Instant.ofEpochMilli(snapshot.end_time + 1));
            CommandReply reply = (CommandReply) latencyHistory.invoke(manager, ClientCommand.newBuilder()
                    .setCommandId("latency-detail")
                    .setCommandType("show_latency_history")
                    .setPayload(ByteString.copyFromUtf8(payload))
                    .build());
            JsonObject body = JsonParser.parseString(reply.getPayload().toStringUtf8()).getAsJsonObject();
            JsonObject metrics = body.getAsJsonArray("snapshots")
                    .get(0).getAsJsonObject().getAsJsonObject("metrics");
            JsonObject search = metrics.getAsJsonObject("Search");

            assertTrue(reply.getSuccess());
            assertEquals(1, body.get("total_snapshots").getAsInt());
            assertEquals(1, search.get("request_count").getAsLong());
            assertFalse(search.has("requestCount"));
            assertFalse(search.has("latencySamplesMicros"));
            assertFalse(search.has("latency_samples_micros"));
            assertFalse(metrics.isJsonArray());
        } finally {
            manager.close();
        }
    }

    @Test
    void latencyAggregateMergesWeightedSamplesInsteadOfWindowP99s() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            long now = System.currentTimeMillis();
            ClientTelemetryManager.OperationSnapshot fast = new ClientTelemetryManager.OperationSnapshot(
                    "Search",
                    new ClientTelemetryManager.MetricSnapshot(
                            100, 100, 0, 1.0, 1.0, 1.0, new long[]{1_000}),
                    Collections.emptyMap());
            ClientTelemetryManager.OperationSnapshot slow = new ClientTelemetryManager.OperationSnapshot(
                    "Search",
                    new ClientTelemetryManager.MetricSnapshot(
                            100, 100, 0, 100.0, 100.0, 100.0, new long[]{100_000}),
                    Collections.emptyMap());
            ClientTelemetryManager.OperationSnapshot legacy = new ClientTelemetryManager.OperationSnapshot(
                    "Legacy",
                    new ClientTelemetryManager.MetricSnapshot(10, 10, 0, 42.0, 42.0, 42.0),
                    Collections.emptyMap());
            @SuppressWarnings("unchecked")
            Deque<ClientTelemetryManager.MetricsSnapshot> snapshots =
                    (Deque<ClientTelemetryManager.MetricsSnapshot>) getField(manager, "snapshots");
            synchronized (snapshots) {
                snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                        now - 20, now - 10, Arrays.asList(fast, legacy)));
                snapshots.addLast(new ClientTelemetryManager.MetricsSnapshot(
                        now - 10, now, Collections.singletonList(slow)));
            }

            String payload = String.format(
                    "{\"start_time\":\"%s\",\"end_time\":\"%s\"}",
                    java.time.Instant.ofEpochMilli(now - 21),
                    java.time.Instant.ofEpochMilli(now + 1));
            CommandReply reply = processOne(manager, command(
                    "weighted-p99", "show_latency_history", payload, 1));
            JsonObject metrics = JsonParser.parseString(reply.getPayload().toStringUtf8())
                    .getAsJsonObject().getAsJsonObject("aggregated").getAsJsonObject("metrics");

            assertTrue(reply.getSuccess());
            assertEquals(200, metrics.getAsJsonObject("Search").get("request_count").getAsLong());
            assertEquals(100.0,
                    metrics.getAsJsonObject("Search").get("p99_latency_ms").getAsDouble());
            assertEquals(42.0,
                    metrics.getAsJsonObject("Legacy").get("p99_latency_ms").getAsDouble());
        } finally {
            manager.close();
        }
    }

    @Test
    void snapshotsRetainAtMost128GlobalLatencySamples() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            assertTrue(processOne(manager, command(
                    "enable-books",
                    "collection_metrics",
                    "{\"enabled\":true,\"collections\":[\"books\"]}",
                    1)).getSuccess());
            for (int latencyMs = 1; latencyMs <= 200; latencyMs++) {
                manager.recordOperation(
                        "Search",
                        "books",
                        System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(latencyMs),
                        "",
                        "");
            }
            invoke(manager, "createSnapshot");
            ClientTelemetryManager.OperationSnapshot search =
                    manager.getMetricsSnapshots().get(0).metrics.get(0);
            Field samplesField = ClientTelemetryManager.MetricSnapshot.class
                    .getDeclaredField("latencySamplesMicros");
            samplesField.setAccessible(true);
            long[] globalSamples = (long[]) samplesField.get(search.global);
            long[] collectionSamples = (long[]) samplesField.get(search.collection_metrics.get("books"));

            assertEquals(128, globalSamples.length);
            assertEquals(0, collectionSamples.length);
            assertEquals(search.global.max_latency_ms, globalSamples[globalSamples.length - 1] / 1000.0);
            for (int index = 1; index < globalSamples.length; index++) {
                assertTrue(globalSamples[index - 1] <= globalSamples[index]);
            }
        } finally {
            manager.close();
        }
    }

    @Test
    void collectionMetricsAreFilteredAgainAtSnapshotAndWireTime() throws Exception {
        ClientTelemetryManager snapshotManager = manager();
        ClientTelemetryManager wireManager = manager();
        try {
            snapshotManager.processCommands(Collections.singletonList(collectionCommand(
                    "enable-snapshot", true, "books", 1)));
            snapshotManager.recordOperation("Search", "books", System.nanoTime(), "", "");
            snapshotManager.processCommands(Collections.singletonList(collectionCommand(
                    "disable-snapshot", false, "books", 2)));
            invoke(snapshotManager, "createSnapshot");
            ClientTelemetryManager.OperationSnapshot snapshot =
                    snapshotManager.getMetricsSnapshots().get(0).metrics.get(0);
            assertEquals(1, snapshot.global.request_count);
            assertTrue(snapshot.collection_metrics.isEmpty());

            wireManager.processCommands(Collections.singletonList(collectionCommand(
                    "enable-wire", true, "books", 1)));
            wireManager.recordOperation("Query", "books", System.nanoTime(), "", "");
            invoke(wireManager, "createSnapshot");
            ClientTelemetryManager.MetricsSnapshot wireSnapshot = wireManager.getMetricsSnapshots().get(0);
            assertFalse(wireSnapshot.metrics.get(0).collection_metrics.isEmpty());
            wireManager.processCommands(Collections.singletonList(collectionCommand(
                    "disable-wire", false, "books", 2)));

            @SuppressWarnings("unchecked")
            List<OperationMetrics> wireMetrics = (List<OperationMetrics>) invoke(
                    wireManager, "toProtoMetrics", List.class, wireSnapshot.metrics);
            assertEquals(1, wireMetrics.get(0).getGlobal().getRequestCount());
            assertTrue(wireMetrics.get(0).getCollectionMetricsMap().isEmpty());
        } finally {
            snapshotManager.close();
            wireManager.close();
        }
    }

    @Test
    void showErrorsUsesGoBoundarySemantics() throws Exception {
        ClientTelemetryManager manager = manager();
        ClientTelemetryManager empty = manager();
        try {
            manager.recordOperation("Query", "books", System.nanoTime(), "first", "");
            manager.recordOperation("Query", "books", System.nanoTime(), "second", "");
            CommandReply fallback = processOne(manager, command(
                    "errors", "show_errors", "{\"max_count\":0}", 1));
            assertEquals(2, JsonParser.parseString(fallback.getPayload().toStringUtf8())
                    .getAsJsonArray().size());

            CommandReply noErrors = processOne(empty, command(
                    "no-errors", "show_errors", "", 1));
            assertTrue(noErrors.getPayload().isEmpty());
        } finally {
            manager.close();
            empty.close();
        }
    }

    @Test
    void truncatesSingleOversizedErrorReply() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            char[] chars = new char[2 * 1024 * 1024];
            Arrays.fill(chars, 'x');
            manager.recordOperation("Query", "books", System.nanoTime(), new String(chars), "");
            java.lang.reflect.Method method = ClientTelemetryManager.class
                    .getDeclaredMethod("handleShowErrors", ClientCommand.class);
            method.setAccessible(true);

            CommandReply reply = (CommandReply) method.invoke(manager, ClientCommand.newBuilder()
                    .setCommandId("errors")
                    .setCommandType("show_errors")
                    .build());

            assertTrue(reply.getSuccess());
            assertTrue(reply.getPayload().size() <= 1024 * 1024);
        } finally {
            manager.close();
        }
    }

    private static ClientTelemetryManager manager() {
        return new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
    }

    private static ClientCommand command(
            String id, String type, String payload, long createTime) {
        return ClientCommand.newBuilder()
                .setCommandId(id)
                .setCommandType(type)
                .setPayload(ByteString.copyFromUtf8(payload))
                .setCreateTime(createTime)
                .build();
    }

    private static ClientCommand collectionCommand(
            String id, boolean enabled, String collection, long createTime) {
        return command(id, "collection_metrics", String.format(
                "{\"enabled\":%s,\"collections\":[\"%s\"]}", enabled, collection), createTime);
    }

    private static CommandReply processOne(
            ClientTelemetryManager manager, ClientCommand command) throws Exception {
        manager.processCommands(Collections.singletonList(command));
        Field field = ClientTelemetryManager.class.getDeclaredField("pendingReplies");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<CommandReply> replies = (List<CommandReply>) field.get(manager);
        synchronized (replies) {
            return replies.get(replies.size() - 1);
        }
    }

    private static Object invoke(Object target, String name) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static Object invoke(
            Object target, String name, Class<?> parameterType, Object argument) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name, parameterType);
        method.setAccessible(true);
        return method.invoke(target, argument);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
