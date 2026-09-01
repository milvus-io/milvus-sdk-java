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

package io.milvus.bulkwriter.connect;

import com.sun.net.httpserver.HttpServer;
import io.minio.credentials.Credentials;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcpMetadataServerCredentialsProviderTest {

    private HttpServer server;
    private final AtomicInteger hitCount = new AtomicInteger();
    private volatile int expiresIn = 3600;
    private volatile int httpStatus = 200;
    private volatile String responseBody;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> {
            hitCount.incrementAndGet();
            String body = responseBody != null ? responseBody
                    : "{\"access_token\":\"token-" + hitCount.get() + "\","
                    + "\"expires_in\":" + expiresIn + ",\"token_type\":\"Bearer\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(httpStatus, httpStatus == 200 ? bytes.length : -1);
            if (httpStatus == 200) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private GcpMetadataServerCredentialsProvider newProvider() {
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/token";
        return new GcpMetadataServerCredentialsProvider(url, new OkHttpClient());
    }

    @Test
    void fetchesTokenFromMetadataServer() {
        Credentials credentials = newProvider().fetch();
        // the bearer token rides in sessionToken; accessKey/secretKey are sentinels
        assertEquals("token-1", credentials.sessionToken());
        assertFalse(credentials.isExpired());
        assertEquals(1, hitCount.get());
    }

    @Test
    void cachesTokenWhileValid() {
        GcpMetadataServerCredentialsProvider provider = newProvider();
        assertEquals("token-1", provider.fetch().sessionToken());
        assertEquals("token-1", provider.fetch().sessionToken());
        assertEquals(1, hitCount.get());
    }

    @Test
    void refetchesWhenTokenNearExpiry() {
        // expires_in below the refresh margin: every call must refetch
        expiresIn = 30;
        GcpMetadataServerCredentialsProvider provider = newProvider();
        assertEquals("token-1", provider.fetch().sessionToken());
        assertEquals("token-2", provider.fetch().sessionToken());
        assertEquals(2, hitCount.get());
    }

    @Test
    void failsFastWhenMetadataServerErrors() {
        httpStatus = 500;
        GcpMetadataServerCredentialsProvider provider = newProvider();
        IllegalStateException e = assertThrows(IllegalStateException.class, provider::fetch);
        assertTrue(e.getMessage().contains("http 500"));
    }

    @Test
    void servesCachedTokenWhenRefetchFailsButTokenStillValid() {
        // token lives 30s: inside the refresh margin, but far from truly expired
        expiresIn = 30;
        GcpMetadataServerCredentialsProvider provider = newProvider();
        assertEquals("token-1", provider.fetch().sessionToken());

        httpStatus = 500;
        // refetch fails, but the cached token is still valid and must be served
        assertEquals("token-1", provider.fetch().sessionToken());
        assertEquals(2, hitCount.get());
    }

    @Test
    void failsWhenTokenExpiredAndRefetchFails() {
        // token expires immediately
        expiresIn = 0;
        GcpMetadataServerCredentialsProvider provider = newProvider();
        provider.fetch();

        httpStatus = 500;
        IllegalStateException e = assertThrows(IllegalStateException.class, provider::fetch);
        assertTrue(e.getMessage().contains("http 500"));
    }

    @Test
    void failsFastWhenResponseIsNotJson() {
        responseBody = "not-json";
        GcpMetadataServerCredentialsProvider provider = newProvider();
        IllegalStateException e = assertThrows(IllegalStateException.class, provider::fetch);
        assertTrue(e.getMessage().contains("unparseable token response"));
    }

    @Test
    void failsFastWhenTokenFieldMissing() {
        responseBody = "{\"token_type\":\"Bearer\"}";
        GcpMetadataServerCredentialsProvider provider = newProvider();
        IllegalStateException e = assertThrows(IllegalStateException.class, provider::fetch);
        assertTrue(e.getMessage().contains("unparseable token response"));
    }
}
