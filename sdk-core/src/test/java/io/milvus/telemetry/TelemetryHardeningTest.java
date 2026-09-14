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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryHardeningTest {

    @Test
    void successfulHandoffClearsUnsupportedBackoff() throws Exception {
        ClientTelemetryManager active = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null);
        ClientTelemetryManager replacement = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null, active.getClientId());
        try {
            setField(active, "unsupportedStreak", 4);
            assertFalse(active.isSupported());

            replacement.prepareRuntimeStateHandoffFrom(active);
            long token = active.beginRuntimeStateRetirement();
            active.handoffRuntimeStateTo(replacement, token);

            // The replacement attaches a fresh transport: the retired endpoint's
            // UNIMPLEMENTED backoff must not carry over, so it probes immediately.
            assertTrue(replacement.isSupported());
            assertEquals(active.getClientId(), replacement.getClientId());
        } finally {
            active.close();
            replacement.close();
        }
    }

    @Test
    void setStubClearsUnsupportedBackoff() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "", null);
        try {
            setField(manager, "unsupportedStreak", 3);
            assertFalse(manager.isSupported());

            // The V2 reconnect path restores runtime state (which carries the streak) and
            // then attaches the fresh transport via setStub: the streak must not survive.
            manager.setStub(null);

            assertTrue(manager.isSupported());
        } finally {
            manager.close();
        }
    }

    @Test
    void snapshotWindowStartIsCappedAtRetentionRange() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.builder().heartbeatIntervalMs(TimeUnit.HOURS.toMillis(3)).build(),
                "", "test", () -> "default", null);
        try {
            manager.recordOperation("Search", "books", System.nanoTime(), "", "");
            invoke(manager, "createSnapshot");

            List<ClientTelemetryManager.MetricsSnapshot> snapshots = manager.getMetricsSnapshots();
            assertEquals(1, snapshots.size());
            ClientTelemetryManager.MetricsSnapshot snapshot = snapshots.get(0);
            // A multi-hour interval would underflow the window start; it must be capped at
            // the retained one-hour history range instead.
            assertEquals(snapshot.end_time - TimeUnit.HOURS.toMillis(1), snapshot.timestamp);
        } finally {
            manager.close();
        }
    }

    @Test
    void truncatedFieldValueCoversAllBranches() throws Exception {
        Method method = ClientTelemetryManager.class.getDeclaredMethod(
                "truncatedFieldValue", String.class);
        method.setAccessible(true);

        // Value not longer than the truncation suffix: emptied.
        assertEquals("", method.invoke(null, "short"));

        // Just past the suffix length: keep collapses to zero, the suffix alone remains.
        assertEquals("...(truncated)", method.invoke(null, repeat('a', 20)));

        // ASCII halving round-trips through UTF-8.
        String ascii = (String) method.invoke(null, repeat('a', 1000));
        assertTrue(ascii.length() < 1000);
        assertTrue(ascii.endsWith("...(truncated)"));
        assertEquals(ascii, new String(ascii.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));

        // A cut that lands on a high surrogate is pulled back so the result stays
        // valid UTF-8 (length 1001 -> keep 486, charAt(485) is a high surrogate).
        String emoji = "A" + repeat("\uD83D\uDE00", 500);
        String surrogate = (String) method.invoke(null, emoji);
        assertEquals(surrogate,
                new String(surrogate.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
    }

    @Test
    void showErrorsTruncatesLongestTruncatableField() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            char[] huge = new char[2 * 1024 * 1024];
            Arrays.fill(huge, 'c');
            manager.recordOperation("Search", new String(huge),
                    System.nanoTime(), "boom", "");
            CommandReply reply = invokeShowErrors(manager);
            assertTrue(reply.getSuccess());
            JsonObject error = JsonParser.parseString(
                    reply.getPayload().toStringUtf8()).getAsJsonArray().get(0).getAsJsonObject();
            assertTrue(error.get("collection").getAsString().length() < huge.length);
            assertEquals("boom", error.get("error_msg").getAsString());
        } finally {
            manager.close();
        }
    }

    @Test
    void canonicalizesAckCommandIdFromCustomHandler() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            manager.registerCommandHandler("custom", command -> CommandReply.newBuilder()
                    .setCommandId("wrong-id")
                    .setSuccess(true)
                    .setPayload(ByteString.copyFromUtf8("hello"))
                    .build());
            ClientCommand cmd = command("canonical", "custom", "", 1);
            CommandReply reply = processOne(manager, cmd);
            // The incoming command ID is the correlation key: the custom handler cannot
            // redirect the ACK.
            assertEquals("canonical", reply.getCommandId());
            assertTrue(reply.getSuccess());
            assertEquals("hello", reply.getPayload().toStringUtf8());
        } finally {
            manager.close();
        }
    }

    @Test
    void duplicateCommandRedeliveryRequeuesOriginalFailedReply() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            ClientCommand bad = command("dup-1", "no_such_type", "", 1);
            CommandReply first = processOne(manager, bad);
            assertFalse(first.getSuccess());
            assertEquals("unknown command type: no_such_type", first.getErrorMessage());

            // Redelivering the same command re-queues the original failed reply instead
            // of minting a contradictory success ACK.
            CommandReply second = processOne(manager, bad);
            assertFalse(second.getSuccess());
            assertEquals(first.getCommandId(), second.getCommandId());
            assertEquals(first.getErrorMessage(), second.getErrorMessage());
        } finally {
            manager.close();
        }
    }

    @Test
    void duplicateNullReplyRedeliveryFallsBackToSuccessAck() throws Exception {
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        try {
            manager.registerCommandHandler("no-reply", command -> null);
            ClientCommand cmd = command("null-1", "no-reply", "", 1);
            manager.processCommands(Collections.singletonList(cmd));
            assertTrue(pendingReplies(manager).isEmpty());

            manager.processCommands(Collections.singletonList(cmd));
            List<CommandReply> replies = pendingReplies(manager);
            assertEquals(1, replies.size());
            assertTrue(replies.get(0).getSuccess());
            assertEquals("null-1", replies.get(0).getCommandId());
        } finally {
            manager.close();
        }
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
        List<CommandReply> replies = pendingReplies(manager);
        return replies.get(replies.size() - 1);
    }

    @SuppressWarnings("unchecked")
    private static List<CommandReply> pendingReplies(ClientTelemetryManager manager) throws Exception {
        Field field = ClientTelemetryManager.class.getDeclaredField("pendingReplies");
        field.setAccessible(true);
        return (List<CommandReply>) field.get(manager);
    }

    private static CommandReply invokeShowErrors(ClientTelemetryManager manager) throws Exception {
        Method method = ClientTelemetryManager.class.getDeclaredMethod(
                "handleShowErrors", ClientCommand.class);
        method.setAccessible(true);
        return (CommandReply) method.invoke(manager, ClientCommand.newBuilder()
                .setCommandId("errors")
                .setCommandType("show_errors")
                .setPayload(ByteString.EMPTY)
                .build());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder(value.length() * count);
        for (int index = 0; index < count; index++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void invoke(Object target, String name) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        method.invoke(target);
    }
}
