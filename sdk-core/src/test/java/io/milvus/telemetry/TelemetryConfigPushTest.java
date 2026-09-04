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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryConfigPushTest {
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
    void repeatedCursorTimestampCommandsRemainDeduplicated() {
        ClientTelemetryManager manager = manager();
        AtomicInteger calls = new AtomicInteger();
        try {
            manager.registerCommandHandler("custom", command -> {
                calls.incrementAndGet();
                return CommandReply.newBuilder()
                        .setCommandId(command.getCommandId())
                        .setSuccess(true)
                        .build();
            });
            List<ClientCommand> commands = Arrays.asList(
                    command("same-a", "custom", "", 7),
                    command("same-b", "custom", "", 7));

            manager.processCommands(commands);
            manager.processCommands(commands);
            manager.processCommands(commands);
            manager.processCommands(Collections.singletonList(
                    command("older", "custom", "", 6)));

            assertEquals(2, calls.get());
            assertEquals(7, manager.getLastCommandTimestamp());
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
}
