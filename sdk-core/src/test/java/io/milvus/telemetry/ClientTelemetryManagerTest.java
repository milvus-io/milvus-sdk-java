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
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.milvus.grpc.ClientCommand;
import io.milvus.grpc.ClientHeartbeatResponse;
import io.milvus.grpc.ClientTelemetryServiceGrpc;
import io.milvus.grpc.CommandReply;
import io.milvus.grpc.OperationMetrics;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientTelemetryManagerTest {
    @Test
    void configHashMatchesCrossSdkVector() {
        ClientCommand commandB = ClientCommand.newBuilder()
                .setCommandId("cfg-b")
                .setCommandType("push_config")
                .setPayload(ByteString.copyFromUtf8("{\"sampling_rate\":0.5}"))
                .setPersistent(true)
                .build();
        ClientCommand commandA = ClientCommand.newBuilder()
                .setCommandId("cfg-a")
                .setCommandType("push_config")
                .setPayload(ByteString.copyFromUtf8("{\"heartbeat_interval_ms\":5000}"))
                .setPersistent(true)
                .build();

        assertEquals("a271ff0bb1941777",
                ClientTelemetryManager.calculateConfigHash(Arrays.asList(commandB, commandA)));
    }

    @Test
    void pushConfigAndCollectionMetricsCommandsAreApplied() {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            manager.processCommands(Arrays.asList(
                    ClientCommand.newBuilder()
                            .setCommandId("config")
                            .setCommandType("push_config")
                            .setPayload(ByteString.copyFromUtf8(
                                    "{\"heartbeat_interval_ms\":5000,\"sampling_rate\":0.25}"))
                            .setCreateTime(1)
                            .setPersistent(true)
                            .build(),
                    ClientCommand.newBuilder()
                            .setCommandId("collections")
                            .setCommandType("collection_metrics")
                            .setPayload(ByteString.copyFromUtf8(
                                    "{\"enabled\":true,\"collections\":[\"books\"]}"))
                            .setCreateTime(2)
                            .build()));

            assertEquals(5000, manager.getConfig().getHeartbeatIntervalMs());
            assertEquals(0.25, manager.getConfig().getSamplingRate());
            assertFalse(manager.getConfigHash().isEmpty());
            assertEquals(2, manager.getLastCommandTimestamp());
            assertTrue(manager.getRecentErrors(10).isEmpty());
        } finally {
            manager.close();
        }
    }

    @Test
    void pushConfigReportsAppliedAndIgnoredKeysInGoOrder() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            CommandReply reply = processOne(manager, command(
                    "config", "push_config",
                    "{\"zzz\":1,\"sampling_rate\":1.5,\"ttl_seconds\":30,"
                            + "\"enabled\":false,\"heartbeat_interval_ms\":5000,\"aaa\":true}", 1));

            assertTrue(reply.getSuccess());
            assertEquals(false, manager.getConfig().isEnabled());
            assertEquals(5000, manager.getConfig().getHeartbeatIntervalMs());
            assertEquals(1.0, manager.getConfig().getSamplingRate());
            JsonObject body = JsonParser.parseString(reply.getPayload().toStringUtf8()).getAsJsonObject();
            assertEquals("[\"enabled\",\"heartbeat_interval_ms\",\"sampling_rate\"]",
                    body.getAsJsonArray("applied").toString());
            assertEquals("[\"aaa\",\"ttl_seconds\",\"zzz\"]",
                    body.getAsJsonArray("ignored").toString());
        } finally {
            manager.close();
        }
    }

    @Test
    void pushConfigValidatesWholePayloadBeforeMutation() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            CommandReply invalidInterval = processOne(manager, command(
                    "bad-interval", "push_config",
                    "{\"enabled\":false,\"heartbeat_interval_ms\":0}", 1));
            assertFalse(invalidInterval.getSuccess());
            assertTrue(manager.getConfig().isEnabled());
            assertEquals(10_000, manager.getConfig().getHeartbeatIntervalMs());

            assertFalse(processOne(manager, command(
                    "string-bool", "push_config", "{\"enabled\":\"false\"}", 2)).getSuccess());
            assertFalse(processOne(manager, command(
                    "fractional-interval", "push_config",
                    "{\"heartbeat_interval_ms\":1.5}", 3)).getSuccess());
            assertFalse(processOne(manager, command(
                    "string-rate", "push_config", "{\"sampling_rate\":\"0.5\"}", 4)).getSuccess());
            assertTrue(manager.getConfig().isEnabled());
            assertEquals(10_000, manager.getConfig().getHeartbeatIntervalMs());
            assertEquals(1.0, manager.getConfig().getSamplingRate());
        } finally {
            manager.close();
        }
    }

    @Test
    void pushConfigStrictlyValidatesIgnoredTtlBeforeMutation() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            List<String> invalidTtlValues = Arrays.asList(
                    "\"30\"", "1.5", "true", "9223372036854775808");
            for (int index = 0; index < invalidTtlValues.size(); index++) {
                CommandReply reply = processOne(manager, command(
                        "bad-ttl-" + index, "push_config",
                        "{\"enabled\":false,\"ttl_seconds\":" + invalidTtlValues.get(index) + "}",
                        index + 1));
                assertFalse(reply.getSuccess());
                assertTrue(manager.getConfig().isEnabled());
            }

            CommandReply nullTtl = processOne(manager, command(
                    "null-ttl", "push_config", "{\"ttl_seconds\":null}", 10));
            assertTrue(nullTtl.getSuccess());
            assertEquals("{\"applied\":[],\"ignored\":[\"ttl_seconds\"]}",
                    nullTtl.getPayload().toStringUtf8());
        } finally {
            manager.close();
        }
    }

    @Test
    void commandPayloadMustBeJsonObject() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            List<String> invalidPayloads = Arrays.asList("[]", "\"text\"", "null", "true");
            for (int index = 0; index < invalidPayloads.size(); index++) {
                assertFalse(processOne(manager, command(
                        "bad-payload-" + index, "show_errors", invalidPayloads.get(index),
                        index + 1)).getSuccess());
            }
        } finally {
            manager.close();
        }
    }

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
    void pushConfigEmptyPayloadReportsEmptyAppliedList() throws Exception {
        ClientTelemetryManager manager = manager();
        try {
            CommandReply reply = processOne(manager, command(
                    "empty-config", "push_config", "", 1));

            assertTrue(reply.getSuccess());
            assertEquals("{\"applied\":[]}", reply.getPayload().toStringUtf8());
        } finally {
            manager.close();
        }
    }

    @Test
    void realBusinessErrorResponseClearsUnsupportedBackoff() throws Exception {
        ClientTelemetryManager manager = manager();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ClientTelemetryServiceGrpc.ClientTelemetryServiceImplBase() {
                    @Override
                    public void clientHeartbeat(
                            io.milvus.grpc.ClientHeartbeatRequest request,
                            StreamObserver<ClientHeartbeatResponse> responseObserver) {
                        responseObserver.onNext(ClientHeartbeatResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.newBuilder()
                                        .setCode(1)
                                        .setReason("not ready")
                                        .build())
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try {
            manager.setStub(ClientTelemetryServiceGrpc.newBlockingStub(channel));
            setField(manager, "unsupportedStreak", 3);

            invoke(manager, "sendHeartbeat");

            assertTrue(manager.isSupported());
            assertEquals("not ready", manager.getLastHeartbeatError().getMessage());
        } finally {
            manager.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void retirementGenerationDropsStaleHeartbeatAndRollbackAllowsNextHeartbeat()
            throws Exception {
        ClientTelemetryManager manager = manager();
        CountDownLatch requestArrived = new CountDownLatch(1);
        CountDownLatch releaseResponse = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ClientTelemetryServiceGrpc.ClientTelemetryServiceImplBase() {
                    @Override
                    public void clientHeartbeat(
                            io.milvus.grpc.ClientHeartbeatRequest request,
                            StreamObserver<ClientHeartbeatResponse> responseObserver) {
                        if (calls.getAndIncrement() == 0) {
                            requestArrived.countDown();
                            try {
                                if (!releaseResponse.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("timed out waiting to release response");
                                }
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(exception);
                            }
                            responseObserver.onNext(ClientHeartbeatResponse.newBuilder()
                                    .setStatus(io.milvus.grpc.Status.getDefaultInstance())
                                    .addCommands(command(
                                            "stale-config",
                                            "push_config",
                                            "{\"enabled\":false}",
                                            10))
                                    .build());
                        } else {
                            responseObserver.onNext(ClientHeartbeatResponse.newBuilder()
                                    .setStatus(io.milvus.grpc.Status.newBuilder()
                                            .setCode(1)
                                            .setReason("after rollback")
                                            .build())
                                    .build());
                        }
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        RuntimeException originalError = new RuntimeException("original");
        try {
            manager.setStub(ClientTelemetryServiceGrpc.newBlockingStub(channel));
            setField(manager, "unsupportedStreak", 3);
            setField(manager, "lastHeartbeatError", originalError);
            Field repliesField = ClientTelemetryManager.class.getDeclaredField("pendingReplies");
            repliesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<CommandReply> pendingReplies = (List<CommandReply>) repliesField.get(manager);
            synchronized (pendingReplies) {
                pendingReplies.add(CommandReply.newBuilder()
                        .setCommandId("pending")
                        .setSuccess(true)
                        .build());
            }
            Future<?> heartbeat = caller.submit(() -> {
                invoke(manager, "sendHeartbeat");
                return null;
            });
            assertTrue(requestArrived.await(5, TimeUnit.SECONDS));

            long retirementToken = manager.beginRuntimeStateRetirement();
            releaseResponse.countDown();
            heartbeat.get(5, TimeUnit.SECONDS);

            assertFalse(manager.isSupported());
            assertSame(originalError, manager.getLastHeartbeatError());
            assertTrue(manager.getConfig().isEnabled());
            assertEquals(0, manager.getLastCommandTimestamp());
            synchronized (pendingReplies) {
                assertEquals(1, pendingReplies.size());
                assertEquals("pending", pendingReplies.get(0).getCommandId());
            }

            manager.cancelRuntimeStateRetirement(retirementToken);
            invoke(manager, "sendHeartbeat");

            assertTrue(manager.isSupported());
            assertEquals("after rollback", manager.getLastHeartbeatError().getMessage());
        } finally {
            releaseResponse.countDown();
            caller.shutdownNow();
            manager.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void processCommandsSerializesAWholeBatch() throws Exception {
        Method method = ClientTelemetryManager.class.getMethod("processCommands", List.class);
        assertTrue(java.lang.reflect.Modifier.isSynchronized(method.getModifiers()));

        ClientTelemetryManager manager = manager();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        try {
            manager.registerCommandHandler("custom", command -> {
                calls.incrementAndGet();
                return CommandReply.newBuilder()
                        .setCommandId(command.getCommandId())
                        .setSuccess(true)
                        .build();
            });
            ClientCommand command = command("same", "custom", "", 1);
            for (int index = 0; index < 2; index++) {
                executor.submit(() -> {
                    start.await();
                    manager.processCommands(Collections.singletonList(command));
                    return null;
                });
            }
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(1, calls.get());
        } finally {
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    void generatesValidClientRequestId() {
        String requestId = ClientTelemetryManager.newClientRequestId();

        assertTrue(requestId.matches("[0-9a-f]{32}"));
        assertFalse(requestId.matches("0{32}"));
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
            assertFalse(metrics.isJsonArray());
        } finally {
            manager.close();
        }
    }

    @Test
    void runtimeStatePreservesPendingRepliesDeduplicationAndCustomHandlers() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ClientTelemetryManager first = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null, "runtime-client");
        ClientTelemetryManager second = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null, "runtime-client");
        try {
            first.registerCommandHandler("custom", command -> {
                calls.incrementAndGet();
                return CommandReply.newBuilder()
                        .setCommandId(command.getCommandId())
                        .setSuccess(true)
                        .build();
            });
            ClientCommand command = ClientCommand.newBuilder()
                    .setCommandId("custom-command")
                    .setCommandType("custom")
                    .setCreateTime(11)
                    .setPersistent(true)
                    .build();
            ClientCommand configCommand = ClientCommand.newBuilder()
                    .setCommandId("config-command")
                    .setCommandType("push_config")
                    .setPayload(ByteString.copyFromUtf8(
                            "{\"heartbeat_interval_ms\":5000,\"sampling_rate\":0.25}"))
                    .setCreateTime(10)
                    .setPersistent(true)
                    .build();
            first.processCommands(Arrays.asList(configCommand, command));
            for (int index = 0; index < 4; index++) {
                first.recordOperation(
                        "Query", "books", System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(2), "", "");
            }
            RuntimeException heartbeatError = new RuntimeException("unimplemented");
            setField(first, "unsupportedStreak", 2);
            setField(first, "lastHeartbeatError", heartbeatError);

            ClientTelemetryManager.RuntimeState state = first.snapshotAndCloseRuntimeState();
            second.restoreRuntimeState(state);

            assertEquals(2, second.snapshotRuntimeState().getPendingReplyCount());
            assertEquals(first.getConfigHash(), second.getConfigHash());
            assertEquals(first.getLastCommandTimestamp(), second.getLastCommandTimestamp());
            assertEquals(5000, second.getConfig().getHeartbeatIntervalMs());
            assertEquals(0.25, second.getConfig().getSamplingRate());
            assertFalse(second.isSupported());
            assertSame(heartbeatError, second.getLastHeartbeatError());
            invoke(second, "createSnapshot");
            assertEquals(1, second.getMetricsSnapshots().get(0).metrics.get(0).global.request_count);
            second.processCommands(Arrays.asList(command));
            assertEquals(1, calls.get());
        } finally {
            first.close();
            second.close();
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
    void telemetryRequestIdPrefersCallOptionAndValidatesBothSources() {
        String callOptionId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String threadId = "0af7651916cd43dd8448eb211c80319c";
        ThreadLocal<String> fallback = new ThreadLocal<>();
        fallback.set(threadId);

        assertEquals(callOptionId, TelemetryInterceptor.requestId(
                CallOptions.DEFAULT.withOption(
                        io.milvus.common.interceptor.ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                        callOptionId),
                fallback));
        assertEquals(threadId, TelemetryInterceptor.requestId(CallOptions.DEFAULT, fallback));
        assertEquals("", TelemetryInterceptor.requestId(
                CallOptions.DEFAULT.withOption(
                        io.milvus.common.interceptor.ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                        ""),
                fallback));
        fallback.set("4BF92F3577B34DA6A3CE929D0E0E4736");
        assertEquals("", TelemetryInterceptor.requestId(CallOptions.DEFAULT, fallback));
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

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
