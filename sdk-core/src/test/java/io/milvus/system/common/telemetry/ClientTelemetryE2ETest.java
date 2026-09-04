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

package io.milvus.system.common.telemetry;
import io.milvus.telemetry.TelemetryConfig;
import io.milvus.telemetry.ClientTelemetryManager;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.RunAnalyzerReq;
import io.milvus.v2.service.vector.response.RunAnalyzerResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "MILVUS_TELEMETRY_E2E", matches = "true")
@Tag("system")
class ClientTelemetryE2ETest {
    private static final Gson GSON = new Gson();
    private static final String MILVUS_URI = System.getenv().getOrDefault(
            "MILVUS_URI", "http://127.0.0.1:19530");
    private static final String TELEMETRY_API = System.getenv().getOrDefault(
            "MILVUS_TELEMETRY_API", "http://127.0.0.1:9091/api/v1/_telemetry");

    @Test
    void defaultTelemetryRegistersAutomatically() throws Exception {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri(MILVUS_URI)
                .build());
        try {
            ClientTelemetryManager manager = client.getTelemetry();
            assertNotNull(manager);
            assertTrue(manager.getConfig().getClientId().isEmpty());
            waitFor("default client registration", manager.getClientId(),
                    state -> "active".equals(state.get("status").getAsString()));
        } finally {
            client.close();
        }
    }

    @Test
    void legacyClientRegistersAutomatically() throws Exception {
        String clientId = "e2e-java-legacy-" + UUID.randomUUID();
        MilvusServiceClient client = new MilvusServiceClient(ConnectParam.newBuilder()
                .withUri(MILVUS_URI)
                .withTelemetryConfig(TelemetryConfig.builder()
                        .heartbeatIntervalMs(500)
                        .clientId(clientId)
                        .build())
                .build());
        try {
            assertEquals(clientId, client.getTelemetry().getClientId());
            waitFor("legacy client registration", clientId,
                    state -> "active".equals(state.get("status").getAsString()));
        } finally {
            client.close();
        }
    }

    @Test
    void telemetryCommandsMetricsAndTracingRoundTrip() throws Exception {
        String clientId = "e2e-java-" + UUID.randomUUID();
        ThreadLocal<String> requestId = new ThreadLocal<>();
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri(MILVUS_URI)
                .clientRequestId(requestId)
                .telemetryConfig(TelemetryConfig.builder()
                        .heartbeatIntervalMs(500)
                        .samplingRate(1.0)
                        .clientId(clientId)
                        .build())
                .build());
        try {
            ClientTelemetryManager manager = client.getTelemetry();
            assertNotNull(manager);
            assertEquals(clientId, manager.getClientId());
            waitFor("client registration", clientId,
                    state -> "active".equals(state.get("status").getAsString()));

            RunAnalyzerResp analyzer = client.runAnalyzer(RunAnalyzerReq.builder()
                    .texts(Collections.singletonList("hello milvus telemetry"))
                    .analyzerParams(Collections.singletonMap("type", "standard"))
                    .withDetail(true)
                    .withHash(false)
                    .build());
            assertEquals(Arrays.asList("hello", "milvus", "telemetry"), Arrays.asList(
                    analyzer.getResults().get(0).getTokens().get(0).getToken(),
                    analyzer.getResults().get(0).getTokens().get(1).getToken(),
                    analyzer.getResults().get(0).getTokens().get(2).getToken()));
            waitFor("RunAnalyzer metric", clientId,
                    state -> hasMetric(state, "RunAnalyzer", "success_count", 1, null));

            JsonObject collectionsPayload = new JsonObject();
            collectionsPayload.add("collections", GSON.toJsonTree(Collections.singletonList("*")));
            collectionsPayload.addProperty("enabled", true);
            String collectionsCommand = pushCommand(clientId, "collection_metrics", collectionsPayload, false);
            JsonObject collectionsReply = waitForReply(clientId, collectionsCommand);
            assertTrue(collectionsReply.get("success").getAsBoolean());

            String traceId = ClientTelemetryManager.newClientRequestId();
            requestId.set(traceId);
            assertThrows(RuntimeException.class, () -> client.query(QueryReq.builder()
                    .collectionName("telemetry_e2e_missing")
                    .filter("id > 0")
                    .build()));
            requestId.remove();
            waitFor("failed Query collection metric", clientId,
                    state -> hasMetric(state, "Query", "error_count", 1, "telemetry_e2e_missing"));

            JsonObject errorsPayload = new JsonObject();
            errorsPayload.addProperty("max_count", 10);
            JsonObject errorsReply = waitForReply(
                    clientId, pushCommand(clientId, "show_errors", errorsPayload, false));
            assertTrue(errorsReply.get("success").getAsBoolean());
            JsonArray errors = JsonParser.parseString(errorsReply.get("payload").getAsString()).getAsJsonArray();
            assertTrue(containsTrace(errors, traceId));

            JsonObject configPayload = new JsonObject();
            configPayload.addProperty("sampling_rate", 0.75);
            configPayload.addProperty("heartbeat_interval_ms", 600);
            String configPayloadJson = GSON.toJson(configPayload);
            String configCommand = pushCommand(clientId, "push_config", configPayload, true);
            JsonObject configReply = waitForReply(clientId, configCommand);
            assertTrue(configReply.get("success").getAsBoolean());
            assertEquals(hash(configCommand + "push_config" + configPayloadJson), manager.getConfigHash());
            assertTrue(manager.getLastCommandTimestamp() > 0);

            JsonObject getConfigReply = waitForReply(
                    clientId, pushCommand(clientId, "get_config", null, false));
            JsonObject userConfig = JsonParser.parseString(getConfigReply.get("payload").getAsString())
                    .getAsJsonObject().getAsJsonObject("user_config");
            assertEquals(0.75, userConfig.get("telemetry_sampling_rate").getAsDouble());
            assertEquals(600, userConfig.get("telemetry_heartbeat_interval_ms").getAsLong());
            assertTrue(userConfig.get("all_collections_enabled").getAsBoolean());
        } finally {
            requestId.remove();
            client.close();
        }
    }

    private static JsonObject waitFor(String label, String clientId, Predicate<JsonObject> predicate)
            throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        JsonObject last = null;
        while (System.currentTimeMillis() < deadline) {
            last = clientState(clientId);
            if (last != null && predicate.test(last)) {
                return last;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("Timed out waiting for " + label + "; last=" + last);
    }

    private static JsonObject waitForReply(String clientId, String commandId) throws Exception {
        JsonObject state = waitFor("command reply " + commandId, clientId,
                candidate -> findReply(candidate, commandId) != null);
        return findReply(state, commandId);
    }

    private static JsonObject clientState(String clientId) throws IOException {
        String query = "client_id=" + URLEncoder.encode(clientId, "UTF-8") + "&include_metrics=true";
        JsonArray clients = request("GET", TELEMETRY_API + "/clients?" + query, null)
                .getAsJsonArray("clients");
        return clients.size() == 0 ? null : clients.get(0).getAsJsonObject();
    }

    private static String pushCommand(
            String clientId, String commandType, JsonElement payload, boolean persistent) throws IOException {
        JsonObject body = new JsonObject();
        body.addProperty("command_type", commandType);
        body.addProperty("target_client_id", clientId);
        body.add("payload", payload == null ? new JsonObject() : payload);
        body.addProperty("ttl_seconds", 30);
        body.addProperty("persistent", persistent);
        return request("POST", TELEMETRY_API + "/commands", body)
                .get("command_id").getAsString();
    }

    private static JsonObject request(String method, String url, JsonObject body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(5_000);
        if (body != null) {
            byte[] bytes = GSON.toJson(body).getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(bytes);
            }
        }
        int status = connection.getResponseCode();
        InputStream input = status >= 200 && status < 300
                ? connection.getInputStream() : connection.getErrorStream();
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        } finally {
            connection.disconnect();
        }
        if (status < 200 || status >= 300) {
            throw new IOException("HTTP " + status + ": " + response);
        }
        return JsonParser.parseString(response.toString()).getAsJsonObject();
    }

    private static JsonObject findReply(JsonObject state, String commandId) {
        JsonArray replies = state.has("command_replies")
                ? state.getAsJsonArray("command_replies") : new JsonArray();
        for (JsonElement element : replies) {
            JsonObject reply = element.getAsJsonObject();
            if (commandId.equals(reply.get("command_id").getAsString())) {
                return reply;
            }
        }
        return null;
    }

    private static boolean hasMetric(
            JsonObject state, String operation, String counter, long minimum, String collection) {
        JsonArray metrics = state.has("metrics") ? state.getAsJsonArray("metrics") : new JsonArray();
        for (JsonElement element : metrics) {
            JsonObject metric = element.getAsJsonObject();
            if (!operation.equals(metric.get("operation").getAsString())) {
                continue;
            }
            JsonObject global = metric.getAsJsonObject("global");
            if (!global.has(counter) || global.get(counter).getAsLong() < minimum) {
                continue;
            }
            if (collection == null) {
                return true;
            }
            return metric.has("collection_metrics")
                    && metric.getAsJsonObject("collection_metrics").has(collection);
        }
        return false;
    }

    private static boolean containsTrace(JsonArray errors, String traceId) {
        for (JsonElement element : errors) {
            JsonObject error = element.getAsJsonObject();
            if (traceId.equals(error.get("request_id").getAsString())
                    && "Query".equals(error.get("operation").getAsString())) {
                return true;
            }
        }
        return false;
    }

    private static String hash(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder(16);
        for (int index = 0; index < 8; index++) {
            result.append(String.format("%02x", digest[index]));
        }
        assertFalse(result.toString().isEmpty());
        return result.toString();
    }
}
