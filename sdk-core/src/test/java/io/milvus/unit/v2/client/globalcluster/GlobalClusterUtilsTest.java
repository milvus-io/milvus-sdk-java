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

package io.milvus.unit.v2.client.globalcluster;

import com.sun.net.httpserver.HttpServer;
import io.milvus.v2.client.globalcluster.ClusterCapability;
import io.milvus.v2.client.globalcluster.GlobalClusterUtils;
import io.milvus.v2.client.globalcluster.GlobalTopology;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GlobalClusterUtilsTest {

    @Test
    void isGlobalEndpointMatchesMarkerCaseInsensitively() {
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://xxx.global-cluster.yyy.com:443"));
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://xxx.GLOBAL-CLUSTER.yyy.com"));
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("http://global-cluster.invalid:19530"));
    }

    @Test
    void isGlobalEndpointRejectsNonGlobalUris() {
        assertFalse(GlobalClusterUtils.isGlobalEndpoint("https://xxx.milvus.yyy.com:19530"));
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(""));
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(null));
    }

    @Test
    void buildTopologyUrlPreservesHostAndPort() throws Exception {
        String url = invokeBuildTopologyUrl("https://xxx.global-cluster.yyy.com:443");

        assertEquals("https://xxx.global-cluster.yyy.com:443/global-cluster/topology", url);
    }

    @Test
    void buildTopologyUrlAddsHttpsSchemeWhenMissing() throws Exception {
        assertEquals("https://host.global-cluster.example:19530/global-cluster/topology",
                invokeBuildTopologyUrl("host.global-cluster.example:19530"));
    }

    @Test
    void buildTopologyUrlUpgradesHttpToHttps() throws Exception {
        assertEquals("https://host.global-cluster.example:19530/global-cluster/topology",
                invokeBuildTopologyUrl("http://host.global-cluster.example:19530"));
    }

    @Test
    void buildTopologyUrlRemovesTrailingSlash() throws Exception {
        assertEquals("https://host.global-cluster.example/global-cluster/topology",
                invokeBuildTopologyUrl("https://host.global-cluster.example/"));
    }

    @Test
    void parseTopologyResponseParsesClusters() throws Exception {
        String json = "{"
                + "\"code\":0,"
                + "\"data\":{"
                + "  \"version\":5,"
                + "  \"clusters\":["
                + "    {\"clusterId\":\"c1\",\"endpoint\":\"host1:19530\",\"capability\":1},"
                + "    {\"clusterId\":\"c2\",\"endpoint\":\"host2:19530\",\"capability\":3}"
                + "  ]"
                + "}"
                + "}";

        GlobalTopology topology = invokeParseTopologyResponse(json);

        assertEquals(5L, topology.getVersion());
        assertEquals(2, topology.getClusters().size());
        assertEquals("c1", topology.getClusters().get(0).getClusterId());
        assertEquals(ClusterCapability.READABLE, topology.getClusters().get(0).getCapability());
        assertEquals("c2", topology.getClusters().get(1).getClusterId());
        assertTrue(topology.getClusters().get(1).isPrimary());
    }

    @Test
    void parseTopologyResponseRejectsErrorCode() throws Exception {
        String json = "{\"code\":1,\"message\":\"boom\"}";

        InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                () -> invokeParseTopologyResponse(json));
        assertTrue(exception.getCause().getMessage().contains("boom"));
    }

    @Test
    void doHttpGetReturnsBodyOnSuccess() throws Exception {
        HttpServer server = startServer(200, "{\"code\":0,\"data\":{\"version\":1,\"clusters\":[]}}",
                new AtomicReference<>());
        try {
            String body = invokeDoHttpGet(
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/global-cluster/topology", null);

            assertEquals("{\"code\":0,\"data\":{\"version\":1,\"clusters\":[]}}", body);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doHttpGetSendsBearerToken() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = startServer(200, "ok", authorization);
        try {
            invokeDoHttpGet("http://127.0.0.1:" + server.getAddress().getPort() + "/topology", "secret");

            assertEquals("Bearer secret", authorization.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doHttpGetThrowsOnNon200() throws Exception {
        HttpServer server = startServer(500, "error", new AtomicReference<>());
        try {
            InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                    () -> invokeDoHttpGet(
                            "http://127.0.0.1:" + server.getAddress().getPort() + "/topology", null));
            assertTrue(exception.getCause() instanceof IOException);
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer startServer(int status, String body, AtomicReference<String> authorization)
            throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
        return server;
    }

    private static String invokeBuildTopologyUrl(String endpoint) throws Exception {
        Method method = GlobalClusterUtils.class.getDeclaredMethod("buildTopologyUrl", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, endpoint);
    }

    private static GlobalTopology invokeParseTopologyResponse(String body) throws Exception {
        Method method = GlobalClusterUtils.class.getDeclaredMethod("parseTopologyResponse", String.class);
        method.setAccessible(true);
        return (GlobalTopology) method.invoke(null, body);
    }

    private static String invokeDoHttpGet(String url, String token) throws Exception {
        Method method = GlobalClusterUtils.class.getDeclaredMethod("doHttpGet", String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, url, token);
    }
}
