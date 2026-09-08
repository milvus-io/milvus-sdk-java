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

package io.milvus.unit.v2.client;

import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.v2.client.ConnectConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("unit")
class ConnectConfigTest {

    @Test
    void builderAppliesAllFields() {
        Map<String, String> option = new HashMap<>();
        option.put("k", "v");
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530/db")
                .token("token123")
                .username("user")
                .password("pw")
                .dbName("configured_db")
                .connectTimeoutMs(1234)
                .keepAliveTimeMs(2345)
                .keepAliveTimeoutMs(3456)
                .keepAliveWithoutCalls(false)
                .rpcDeadlineMs(1000)
                .clientKeyPath("/key")
                .clientPemPath("/client.pem")
                .caPemPath("/ca.pem")
                .serverPemPath("/server.pem")
                .serverName("server-name")
                .proxyAddress("proxy:8080")
                .secure(false)
                .enablePrecheck(true)
                .idleTimeoutMs(5000)
                .telemetryClientId("client-id")
                .deferTelemetryStart(true)
                .option(option)
                .build();

        assertEquals("http://localhost:19530/db", config.getUri());
        assertEquals("token123", config.getToken());
        assertEquals("user", config.getUsername());
        assertEquals("pw", config.getPassword());
        assertEquals("db", config.getDbName());
        assertEquals(1234, config.getConnectTimeoutMs());
        assertEquals(2345, config.getKeepAliveTimeMs());
        assertEquals(3456, config.getKeepAliveTimeoutMs());
        assertFalse(config.isKeepAliveWithoutCalls());
        assertEquals(1000, config.getRpcDeadlineMs());
        assertEquals("/key", config.getClientKeyPath());
        assertEquals("/client.pem", config.getClientPemPath());
        assertEquals("/ca.pem", config.getCaPemPath());
        assertEquals("/server.pem", config.getServerPemPath());
        assertEquals("server-name", config.getServerName());
        assertEquals("proxy:8080", config.getProxyAddress());
        assertFalse(config.getSecure());
        assertTrue(config.isEnablePrecheck());
        assertEquals(5000, config.getIdleTimeoutMs());
        assertEquals("client-id", config.getTelemetryClientId());
        assertTrue(config.isDeferTelemetryStart());
        assertEquals("v", config.getOption().get("k"));
    }

    @Test
    void builderDefaultsMatchDocumentation() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();

        assertEquals(10000, config.getConnectTimeoutMs());
        assertEquals(10000, config.getKeepAliveTimeMs());
        assertEquals(5000, config.getKeepAliveTimeoutMs());
        assertTrue(config.isKeepAliveWithoutCalls());
        assertEquals(0, config.getRpcDeadlineMs());
        assertFalse(config.getSecure());
        assertFalse(config.isEnablePrecheck());
        assertEquals(TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS), config.getIdleTimeoutMs());
        assertEquals("", config.getTelemetryClientId());
        assertFalse(config.isDeferTelemetryStart());
        assertNotNull(config.getTelemetryConfig());
    }

    @Test
    void hostPortAndDbNameAreParsedFromUri() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example:19530/some_db")
                .dbName("ignored")
                .build();

        assertEquals("host.example", config.getHost());
        assertEquals(19530, config.getPort());
        assertEquals("some_db", config.getDbName());
    }

    @Test
    void dbNameFallsBackToConfiguredValueWhenUriHasNoPath() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example:19530")
                .dbName("fallback_db")
                .build();

        assertEquals("fallback_db", config.getDbName());
    }

    @Test
    void serverlessUriUsesPort443() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("https://in03-aaaaaaaaaaaaaaaaaaaa-zilliz.com")
                .build();

        assertEquals(443, config.getPort());
    }

    @Test
    void defaultPortIs19530WhenAbsent() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example")
                .build();

        assertEquals(19530, config.getPort());
    }

    @Test
    void authorizationPrefersToken() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example")
                .token("token123")
                .username("user")
                .password("pw")
                .build();

        assertEquals("token123", config.getAuthorization());
    }

    @Test
    void authorizationFallsBackToUserPassword() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example")
                .username("user")
                .password("pw")
                .build();

        assertEquals("user:pw", config.getAuthorization());
    }

    @Test
    void authorizationIsNullWithoutCredentials() {
        ConnectConfig config = ConnectConfig.builder().uri("http://host.example").build();

        assertNull(config.getAuthorization());
    }

    @Test
    void isSecureTrueForHttpsUri() {
        ConnectConfig config = ConnectConfig.builder().uri("https://host.example").build();

        assertTrue(config.isSecure());
    }

    @Test
    void isSecureFallsBackToConfiguredFlag() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example")
                .secure(true)
                .build();

        assertTrue(config.isSecure());
    }

    @Test
    void settersUpdateGetters() {
        ConnectConfig config = ConnectConfig.builder().uri("http://host.example").build();
        config.setUri("http://other.example:1234");
        config.setToken("t");
        config.setUsername("u");
        config.setPassword("p");
        config.setDbName("d");
        config.setConnectTimeoutMs(1);
        config.setKeepAliveTimeMs(2);
        config.setKeepAliveTimeoutMs(3);
        config.setKeepAliveWithoutCalls(false);
        config.setRpcDeadlineMs(4);
        config.setClientKeyPath("/k");
        config.setClientPemPath("/cp");
        config.setCaPemPath("/ca");
        config.setServerPemPath("/sp");
        config.setServerName("sn");
        config.setProxyAddress("proxy:1");
        config.setSecure(true);
        config.setEnablePrecheck(true);
        config.setIdleTimeoutMs(5);
        config.setTelemetryClientId(null);

        assertEquals("http://other.example:1234", config.getUri());
        assertEquals("t", config.getToken());
        assertEquals("u", config.getUsername());
        assertEquals("p", config.getPassword());
        assertEquals("d", config.getDbName());
        assertEquals(1, config.getConnectTimeoutMs());
        assertEquals(2, config.getKeepAliveTimeMs());
        assertEquals(3, config.getKeepAliveTimeoutMs());
        assertFalse(config.isKeepAliveWithoutCalls());
        assertEquals(4, config.getRpcDeadlineMs());
        assertEquals("sn", config.getServerName());
        assertTrue(config.getSecure());
        assertTrue(config.isEnablePrecheck());
        assertEquals("", config.getTelemetryClientId());
    }

    @Test
    void telemetryRuntimeStateIsTakenOnce() {
        ClientTelemetryManager.RuntimeState state = mock(ClientTelemetryManager.RuntimeState.class);
        ConnectConfig config = ConnectConfig.builder().uri("http://host.example").build();
        config.setTelemetryRuntimeState(state);

        assertEquals(state, config.takeTelemetryRuntimeState());
        assertNull(config.takeTelemetryRuntimeState());
    }

    @Test
    void buildRequiresUri() {
        assertThrows(NullPointerException.class, () -> ConnectConfig.builder().build());
    }

    @Test
    void uriSetterRejectsNull() {
        ConnectConfig config = ConnectConfig.builder().uri("http://host.example").build();

        assertThrows(NullPointerException.class, () -> config.setUri(null));
    }

    @Test
    void usernameRejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> ConnectConfig.builder()
                .uri("http://host.example")
                .username("  ")
                .build());
    }

    @Test
    void toStringRedactsCredentials() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://host.example")
                .token("secret-token")
                .password("secret-pw")
                .build();

        String value = config.toString();
        assertTrue(value.contains("http://host.example"));
        assertFalse(value.contains("secret-token"));
        assertFalse(value.contains("secret-pw"));
    }
}
