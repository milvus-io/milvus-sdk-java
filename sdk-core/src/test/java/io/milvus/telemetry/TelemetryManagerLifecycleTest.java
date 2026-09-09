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
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.milvus.grpc.ClientCommand;
import io.milvus.grpc.ClientHeartbeatRequest;
import io.milvus.grpc.ClientHeartbeatResponse;
import io.milvus.grpc.ClientTelemetryServiceGrpc;
import io.milvus.grpc.CommandReply;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryManagerLifecycleTest {
    @Test
    void startSchedulingFailureIsBestEffortAndClosedManagerStaysStopped() throws Exception {
        ClientTelemetryManager rejected = manager();
        ClientTelemetryManager closed = manager();
        try {
            ScheduledExecutorService executor =
                    (ScheduledExecutorService) getField(rejected, "executor");
            executor.shutdownNow();

            rejected.start();
            assertFalse(rejected.isReady());
            assertTrue(rejected.getLastHeartbeatError() instanceof RejectedExecutionException);
            rejected.start();
            assertFalse(rejected.isReady());

            closed.close();
            closed.start();
            assertFalse(closed.isReady());
            assertTrue(closed.isClosed());
        } finally {
            rejected.close();
            closed.close();
        }
    }

    @Test
    void dynamicDisableKeepsControlPlaneActivatedAcrossRuntimeStateHandoff() throws Exception {
        ClientTelemetryManager active = new ClientTelemetryManager(
                TelemetryConfig.builder().heartbeatIntervalMs(60_000).build(),
                "", "test", () -> "", null);
        ClientCommand disable = ClientCommand.newBuilder()
                .setCommandId("disable")
                .setCommandType("push_config")
                .setPayload(ByteString.copyFromUtf8("{\"enabled\":false}"))
                .setCreateTime(1)
                .setPersistent(true)
                .build();
        CountDownLatch requestReceived = new CountDownLatch(1);
        AtomicReference<ClientHeartbeatRequest> captured = new AtomicReference<>();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ClientTelemetryServiceGrpc.ClientTelemetryServiceImplBase() {
                    @Override
                    public void clientHeartbeat(
                            ClientHeartbeatRequest request,
                            StreamObserver<ClientHeartbeatResponse> responseObserver) {
                        captured.set(request);
                        responseObserver.onNext(ClientHeartbeatResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.getDefaultInstance())
                                .build());
                        responseObserver.onCompleted();
                        requestReceived.countDown();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        ClientTelemetryManager replacement = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null, active.getClientId());
        try {
            active.start();
            active.processCommands(Collections.singletonList(disable));
            assertFalse(active.getConfig().isEnabled());

            replacement.restoreRuntimeState(active.snapshotRuntimeState());
            replacement.setStub(ClientTelemetryServiceGrpc.newBlockingStub(channel));
            replacement.start();

            assertTrue(requestReceived.await(2, TimeUnit.SECONDS));
            assertFalse(replacement.getConfig().isEnabled());
            assertEquals(0, captured.get().getMetricsCount());
            assertEquals("disable", captured.get().getCommandReplies(0).getCommandId());
            assertEquals(active.getConfigHash(), captured.get().getConfigHash());
        } finally {
            active.close();
            replacement.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void lifecycleSamplingErrorBoundsAndHandoffValidation() throws Exception {
        TelemetryConfig disabledConfig = TelemetryConfig.builder()
                .enabled(false)
                .samplingRate(0.0)
                .errorMaxCount(1)
                .clientId("stable-client")
                .build();
        ClientTelemetryManager disabled = new ClientTelemetryManager(
                disabledConfig, null, null, null, null);
        ClientTelemetryManager active = manager();
        ClientTelemetryManager replacement = manager();
        ClientTelemetryManager wrongClient = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null, "wrong-client");
        ClientTelemetryManager zeroSampling = new ClientTelemetryManager(
                TelemetryConfig.builder().samplingRate(0.0).build(), "", "", null, () -> null);
        ClientTelemetryManager boundedErrors = new ClientTelemetryManager(
                TelemetryConfig.builder().errorMaxCount(1).build(), "", "", null, null);
        ClientTelemetryManager nullConfig = new ClientTelemetryManager(
                null, null, null, null, null, null);
        try {
            disabled.start();
            assertTrue(disabled.isReady());
            Field activationField = ClientTelemetryManager.class.getDeclaredField(
                    "controlPlaneActivated");
            activationField.setAccessible(true);
            assertFalse(((AtomicBoolean) activationField.get(disabled)).get());
            assertEquals("stable-client", disabled.getClientId());
            disabled.recordOperation("Search", "books", System.nanoTime(), "ignored", "id");
            invoke(disabled, "createSnapshot");
            assertTrue(disabled.getMetricsSnapshots().isEmpty());
            disabled.restoreRuntimeState(null);

            zeroSampling.recordOperation(
                    "Search", "books", System.nanoTime(), "not sampled", "request");
            assertTrue(zeroSampling.getRecentErrors(10).isEmpty());
            boundedErrors.recordOperation(
                    "Query", "books", System.nanoTime(), "first", "id-1");
            boundedErrors.recordOperation(
                    "Query", "books", System.nanoTime(), "second", "id-2");
            assertEquals(1, boundedErrors.getRecentErrors(10).size());
            assertEquals("second", boundedErrors.getRecentErrors(10).get(0).error_msg);
            assertFalse(nullConfig.getClientId().isEmpty());
            assertEquals("", ClientTelemetryManager.calculateConfigHash(Collections.emptyList()));
            assertFalse(processOne(active, command(
                    "unknown", "not_registered", "", 1)).getSuccess());
            for (int index = 0; index <= 120; index++) {
                invoke(active, "createSnapshot");
            }
            assertEquals(121, active.getMetricsSnapshots().size());

            active.recordOperation("Query", "books", System.nanoTime(), "first", "id-1");
            active.recordOperation("Query", "books", System.nanoTime(), "second", "id-2");
            assertEquals(1, active.getRecentErrors(1).size());

            assertThrows(IllegalArgumentException.class,
                    () -> wrongClient.restoreRuntimeState(active.snapshotRuntimeState()));
            assertThrows(IllegalArgumentException.class,
                    () -> replacement.prepareRuntimeStateHandoffFrom(null));
            replacement.start();
            assertThrows(IllegalStateException.class,
                    () -> replacement.prepareRuntimeStateHandoffFrom(active));
            assertThrows(IllegalArgumentException.class,
                    () -> active.handoffRuntimeStateTo(null, 1));
            assertThrows(IllegalStateException.class,
                    () -> active.handoffRuntimeStateTo(replacement, 1));
            long token = active.beginRuntimeStateRetirement();
            assertThrows(IllegalStateException.class, active::beginRuntimeStateRetirement);
            invoke(active, "sendHeartbeat");
            active.cancelRuntimeStateRetirement(token + 1);
            active.cancelRuntimeStateRetirement(token);
            assertThrows(IllegalStateException.class,
                    () -> active.handoffRuntimeStateTo(wrongClient, token));
        } finally {
            disabled.close();
            active.close();
            replacement.close();
            wrongClient.close();
            zeroSampling.close();
            boundedErrors.close();
            nullConfig.close();
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

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
