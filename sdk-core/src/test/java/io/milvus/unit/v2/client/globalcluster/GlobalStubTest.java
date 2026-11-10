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

import io.milvus.telemetry.ClientTelemetryManager;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.globalcluster.ClusterCapability;
import io.milvus.v2.client.globalcluster.ClusterInfo;
import io.milvus.v2.client.globalcluster.GlobalStub;
import io.milvus.v2.client.globalcluster.GlobalTopology;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Tag("unit")
class GlobalStubTest {
    private static final String GLOBAL_ENDPOINT = "https://host.global-cluster.example:443";

    private static ClusterInfo primaryInfo(String endpoint) {
        return new ClusterInfo("c1", endpoint, ClusterCapability.PRIMARY);
    }

    private static GlobalTopology topology(long version, String endpoint) {
        return new GlobalTopology(version, Collections.singletonList(primaryInfo(endpoint)));
    }

    private static GlobalStub stub(GlobalTopology topology, MilvusClientV2 innerClient, Object clientFactory)
            throws Exception {
        return stub(topology, innerClient, clientFactory, c -> { });
    }

    private static GlobalStub stub(GlobalTopology topology, MilvusClientV2 innerClient,
                                   Object clientFactory, Consumer<MilvusClientV2> onPrimaryChange)
            throws Exception {
        Constructor<?> constructor = null;
        for (Constructor<?> candidate : GlobalStub.class.getDeclaredConstructors()) {
            if (candidate.getParameterCount() == 6) {
                constructor = candidate;
                break;
            }
        }
        if (constructor == null) {
            throw new IllegalStateException("package-private GlobalStub constructor not found");
        }
        constructor.setAccessible(true);
        ConnectConfig config = ConnectConfig.builder().uri(GLOBAL_ENDPOINT).build();
        return (GlobalStub) constructor.newInstance(
                GLOBAL_ENDPOINT, config, onPrimaryChange, topology,
                innerClient, clientFactory);
    }

    private static Object clientFactoryReturning(MilvusClientV2 newClient) throws Exception {
        ClassLoader loader = GlobalStubTest.class.getClassLoader();
        Class<?> factoryType = Class.forName("io.milvus.v2.client.globalcluster.GlobalStub$ClientFactory");
        return Proxy.newProxyInstance(loader, new Class<?>[]{factoryType}, (proxy, method, args) -> {
            if (method.getName().equals("create")) {
                return newClient;
            }
            return null;
        });
    }

    private static boolean onTopologyChange(GlobalStub stub, GlobalTopology newTopology) throws Exception {
        Method method = GlobalStub.class.getDeclaredMethod("onTopologyChange", GlobalTopology.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(stub, newTopology);
    }

    private static ConnectConfig clonePrimaryConfig(ConnectConfig original, String endpoint)
            throws Exception {
        Method method = GlobalStub.class.getDeclaredMethod("cloneConfigWithNewUri",
                ConnectConfig.class, String.class, ClientTelemetryManager.RuntimeState.class,
                boolean.class);
        method.setAccessible(true);
        try {
            return (ConnectConfig) method.invoke(null, original, endpoint, null, false);
        } catch (InvocationTargetException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    @Test
    void gettersExposeConstructorValues() throws Exception {
        GlobalTopology topology = topology(1L, "host1:19530");
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology, inner, clientFactoryReturning(mock(MilvusClientV2.class)));

        assertSame(inner, stub.getPrimaryClient());
        assertSame(topology, stub.getTopology());
        assertEquals("host1:19530", stub.getPrimaryEndpoint());
    }

    @Test
    void triggerRefreshIsNoOpWithoutRefresher() throws Exception {
        GlobalStub stub = stub(topology(1L, "host1:19530"), mock(MilvusClientV2.class),
                clientFactoryReturning(mock(MilvusClientV2.class)));

        stub.triggerRefresh();
    }

    @Test
    void retargetPrimaryChangePublishesCurrentPrimary() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "host1:19530"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));
        List<MilvusClientV2> published = new ArrayList<>();

        stub.retargetPrimaryChange(published::add);

        assertEquals(Collections.singletonList(inner), published);
    }

    @Test
    void retargetPrimaryChangePublishesSwappedPrimary() throws Exception {
        MilvusClientV2 oldClient = mock(MilvusClientV2.class);
        MilvusClientV2 newClient = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "host1:19530"), oldClient, clientFactoryReturning(newClient));
        List<MilvusClientV2> published = new ArrayList<>();
        stub.retargetPrimaryChange(published::add);

        onTopologyChange(stub, topology(2L, "host2:19530"));

        assertEquals(2, published.size());
        assertSame(oldClient, published.get(0));
        assertSame(newClient, published.get(1));
    }

    @Test
    void retargetPrimaryChangeRejectsNullCallback() throws Exception {
        GlobalStub stub = stub(topology(1L, "host1:19530"), mock(MilvusClientV2.class),
                clientFactoryReturning(mock(MilvusClientV2.class)));

        assertThrows(IllegalArgumentException.class, () -> stub.retargetPrimaryChange(null));
    }

    @Test
    void topologyVersionChangeWithSamePrimaryKeepsClient() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "host1:19530"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));

        assertTrue(onTopologyChange(stub, topology(2L, "host1:19530")));

        assertSame(inner, stub.getPrimaryClient());
        assertEquals(2L, stub.getTopology().getVersion());
        verify(inner, never()).close();
    }

    @Test
    void staleTopologyVersionIsRejected() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(2L, "host1:19530"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));

        // Memory only advances: a lower (or equal) version must never roll the topology back,
        // even when it points at a different primary.
        assertFalse(onTopologyChange(stub, topology(1L, "host2:19530")));
        assertFalse(onTopologyChange(stub, topology(2L, "host2:19530")));

        assertSame(inner, stub.getPrimaryClient());
        assertEquals("host1:19530", stub.getPrimaryEndpoint());
        assertEquals(2L, stub.getTopology().getVersion());
        verify(inner, never()).close();
    }

    @Test
    void onTopologyChangeAfterCloseIsRejected() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "host1:19530"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));
        stub.close();

        assertFalse(onTopologyChange(stub, topology(2L, "host2:19530")));
        assertNull(stub.getPrimaryClient());
    }

    @Test
    void primaryChangeSwapsClientAndClosesOldClient() throws Exception {
        MilvusClientV2 oldClient = mock(MilvusClientV2.class);
        MilvusClientV2 newClient = mock(MilvusClientV2.class);
        List<MilvusClientV2> published = new ArrayList<>();
        GlobalStub stub = stub(topology(1L, "host1:19530"), oldClient, clientFactoryReturning(newClient),
                published::add);

        GlobalTopology newTopology = topology(3L, "host2:19530");
        onTopologyChange(stub, newTopology);

        assertSame(newClient, stub.getPrimaryClient());
        assertEquals("host2:19530", stub.getPrimaryEndpoint());
        assertSame(newTopology, stub.getTopology());
        assertEquals(Collections.singletonList(newClient), published);
        verify(oldClient).close();
    }

    @Test
    void equivalentPrimaryAddressDoesNotSwapClient() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "https://PRIMARY.example.com"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));

        GlobalTopology newer = topology(2L, "https://primary.example.com:443");
        assertTrue(onTopologyChange(stub, newer));

        assertSame(inner, stub.getPrimaryClient());
        assertSame(newer, stub.getTopology());
        verify(inner, never()).close();
    }

    @Test
    void primaryConfigUsesSchemeAwareDefaultPorts() throws Exception {
        ConnectConfig secureOriginal = ConnectConfig.builder().uri(GLOBAL_ENDPOINT).build();

        ConnectConfig https = clonePrimaryConfig(secureOriginal, "primary.example.com");
        assertEquals("https://primary.example.com:443", https.getUri());
        assertEquals(443, https.getPort());
        assertTrue(https.isSecure());

        ConnectConfig explicit = clonePrimaryConfig(
                secureOriginal, "https://primary.example.com:8443");
        assertEquals("https://primary.example.com:8443", explicit.getUri());
        assertEquals(8443, explicit.getPort());

        ConnectConfig http = clonePrimaryConfig(secureOriginal, "http://primary.example.com");
        assertEquals("http://primary.example.com:19530", http.getUri());
        assertEquals(19530, http.getPort());
        assertFalse(http.isSecure());

        ConnectConfig insecureOriginal = ConnectConfig.builder()
                .uri("http://host.global-cluster.example:19530")
                .build();
        ConnectConfig inherited = clonePrimaryConfig(insecureOriginal, "primary.example.com");
        assertEquals("http://primary.example.com:19530", inherited.getUri());
        assertFalse(inherited.isSecure());

        assertThrows(IllegalArgumentException.class,
                () -> clonePrimaryConfig(secureOriginal, null));
        assertThrows(IllegalArgumentException.class,
                () -> clonePrimaryConfig(secureOriginal, "  "));
        assertThrows(IllegalArgumentException.class,
                () -> clonePrimaryConfig(secureOriginal, "ftp://primary.example.com"));
        assertThrows(IllegalArgumentException.class,
                () -> clonePrimaryConfig(secureOriginal, "https://"));
        assertThrows(IllegalArgumentException.class,
                () -> clonePrimaryConfig(secureOriginal, "https://bad host"));
    }

    @Test
    void closeClosesInnerClient() throws Exception {
        MilvusClientV2 inner = mock(MilvusClientV2.class);
        GlobalStub stub = stub(topology(1L, "host1:19530"), inner,
                clientFactoryReturning(mock(MilvusClientV2.class)));

        stub.close();

        verify(inner).close();
        assertNull(stub.getPrimaryClient());
    }
}
