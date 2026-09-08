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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryHeartbeatTest {
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
    void successfulHeartbeatConsumesRepliesAndAppliesCommands() throws Exception {
        AtomicReference<io.milvus.grpc.ClientHeartbeatRequest> captured = new AtomicReference<>();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ClientTelemetryServiceGrpc.ClientTelemetryServiceImplBase() {
                    @Override
                    public void clientHeartbeat(
                            io.milvus.grpc.ClientHeartbeatRequest request,
                            StreamObserver<ClientHeartbeatResponse> responseObserver) {
                        captured.set(request);
                        responseObserver.onNext(ClientHeartbeatResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.getDefaultInstance())
                                .addCommands(command(
                                        "server-config", "push_config",
                                        "{\"heartbeat_interval_ms\":4321}", 2))
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "user", "test", () -> null, null);
        try {
            processOne(manager, command("local-reply", "show_errors", "", 1));
            manager.setStub(ClientTelemetryServiceGrpc.newBlockingStub(channel));

            invoke(manager, "sendHeartbeat");

            assertEquals(1, captured.get().getCommandRepliesCount());
            assertFalse(captured.get().getClientInfo().getReservedMap().containsKey("db_name"));
            // The sent local reply is consumed and the newly applied server command queues its
            // own acknowledgement for the next heartbeat.
            assertEquals(1, manager.snapshotRuntimeState().getPendingReplyCount());
            assertEquals(4321, manager.getConfig().getHeartbeatIntervalMs());
            assertEquals(2, manager.getLastCommandTimestamp());
            assertTrue(manager.isSupported());
        } finally {
            manager.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void disabledMetricsKeepsScheduledControlPlaneAndCanBeReenabled() throws Exception {
        List<ClientHeartbeatRequest> captured = new CopyOnWriteArrayList<>();
        CountDownLatch requests = new CountDownLatch(3);
        AtomicInteger calls = new AtomicInteger();
        ClientCommand disable = ClientCommand.newBuilder()
                .setCommandId("disable")
                .setCommandType("push_config")
                .setPayload(ByteString.copyFromUtf8("{\"enabled\":false}"))
                .setCreateTime(1)
                .setPersistent(true)
                .build();
        ClientCommand enable = ClientCommand.newBuilder()
                .setCommandId("enable")
                .setCommandType("push_config")
                .setPayload(ByteString.copyFromUtf8("{\"enabled\":true}"))
                .setCreateTime(2)
                .setPersistent(true)
                .build();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ClientTelemetryServiceGrpc.ClientTelemetryServiceImplBase() {
                    @Override
                    public void clientHeartbeat(
                            ClientHeartbeatRequest request,
                            StreamObserver<ClientHeartbeatResponse> responseObserver) {
                        captured.add(request);
                        int call = calls.incrementAndGet();
                        ClientHeartbeatResponse.Builder response = ClientHeartbeatResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.getDefaultInstance());
                        if (call == 1) {
                            response.addCommands(disable);
                        } else if (call == 2) {
                            response.addCommands(enable);
                        }
                        responseObserver.onNext(response.build());
                        responseObserver.onCompleted();
                        requests.countDown();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.builder().heartbeatIntervalMs(5).build(),
                "", "test", () -> "", null);
        try {
            manager.recordOperation("Query", "books", System.nanoTime(), "", "");
            manager.setStub(ClientTelemetryServiceGrpc.newBlockingStub(channel));
            manager.start();

            assertTrue(requests.await(2, TimeUnit.SECONDS));
            assertEquals(1, captured.get(0).getMetricsCount());
            assertEquals(0, captured.get(1).getMetricsCount());
            assertEquals("disable", captured.get(1).getCommandReplies(0).getCommandId());
            assertEquals(
                    ClientTelemetryManager.calculateConfigHash(Collections.singletonList(disable)),
                    captured.get(1).getConfigHash());
            assertEquals("enable", captured.get(2).getCommandReplies(0).getCommandId());
            assertEquals(
                    ClientTelemetryManager.calculateConfigHash(Collections.singletonList(enable)),
                    captured.get(2).getConfigHash());
            assertTrue(manager.getConfig().isEnabled());
        } finally {
            manager.close();
            channel.shutdownNow();
            server.shutdownNow();
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

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
