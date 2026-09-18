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
import io.milvus.v2.client.globalcluster.GlobalClusterApiException;
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
    void isGlobalEndpointMatchesMarkerAndRejectsOthers() {
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://xxx.global-cluster.yyy.com:443"));
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("https://xxx.GLOBAL-CLUSTER.yyy.com"));
        assertTrue(GlobalClusterUtils.isGlobalEndpoint("http://global-cluster.invalid:19530"));

        assertFalse(GlobalClusterUtils.isGlobalEndpoint("https://xxx.milvus.yyy.com:19530"));
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(""));
        assertFalse(GlobalClusterUtils.isGlobalEndpoint(null));
    }

    @Test
    void parseTopologyResponseParsesClustersAndRejectsErrorCode() throws Throwable {
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
        assertEquals("c2", topology.getClusters().get(1).getClusterId());
        assertTrue(topology.getClusters().get(1).isPrimary());

        GlobalClusterApiException exception = assertThrows(GlobalClusterApiException.class,
                () -> invokeParseTopologyResponse("{\"code\":7,\"message\":\"boom\"}"));
        assertEquals(7, exception.getCode());
        assertTrue(exception.getMessage().contains("boom"));
    }

    @Test
    void doHttpGetReturnsBodySendsTokenAndThrowsOnNon200() throws Throwable {
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer okServer = startServer(200, "{\"code\":0,\"data\":{\"version\":1,\"clusters\":[]}}", authorization);
        try {
            String body = invokeDoHttpGet(
                    "http://127.0.0.1:" + okServer.getAddress().getPort() + "/topology", "secret");
            assertEquals("{\"code\":0,\"data\":{\"version\":1,\"clusters\":[]}}", body);
            assertEquals("Bearer secret", authorization.get());
        } finally {
            okServer.stop(0);
        }

        HttpServer errorServer = startServer(500, "error", new AtomicReference<>());
        try {
            IOException exception = assertThrows(IOException.class,
                    () -> invokeDoHttpGet(
                            "http://127.0.0.1:" + errorServer.getAddress().getPort() + "/topology", null));
            assertTrue(exception.getMessage().contains("500"));
        } finally {
            errorServer.stop(0);
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

    private static GlobalTopology invokeParseTopologyResponse(String body) throws Throwable {
        try {
            Method method = GlobalClusterUtils.class.getDeclaredMethod("parseTopologyResponse", String.class);
            method.setAccessible(true);
            return (GlobalTopology) method.invoke(null, body);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static String invokeDoHttpGet(String url, String token) throws Throwable {
        try {
            Method method = GlobalClusterUtils.class.getDeclaredMethod("doHttpGet", String.class, String.class);
            method.setAccessible(true);
            return (String) method.invoke(null, url, token);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
