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
import io.milvus.grpc.CommandReply;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void generatesValidClientRequestId() {
        String requestId = ClientTelemetryManager.newClientRequestId();

        assertTrue(requestId.matches("[0-9a-f]{32}"));
        assertFalse(requestId.matches("0{32}"));
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
}
