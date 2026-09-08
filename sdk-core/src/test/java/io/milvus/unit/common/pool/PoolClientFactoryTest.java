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

package io.milvus.unit.common.pool;

import io.milvus.pool.PoolClientFactory;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.apache.commons.pool2.PooledObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Objects;

@Tag("unit")
class PoolClientFactoryTest {

    public static class TestConfig {
        final String id;
        final boolean failOnCreate;

        TestConfig(String id) {
            this(id, false);
        }

        TestConfig(String id, boolean failOnCreate) {
            this.id = id;
            this.failOnCreate = failOnCreate;
        }
    }

    public static class TestClient {
        final String id;
        int closeCalls = 0;
        int readyCalls = 0;
        boolean ready = true;

        public TestClient(TestConfig config) {
            this.id = config.id;
            if (config.failOnCreate) {
                throw new RuntimeException("simulated constructor failure");
            }
        }

        public void close(long maxWaitSeconds) {
            closeCalls++;
        }

        public boolean clientIsReady() {
            readyCalls++;
            return ready;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestClient)) {
                return false;
            }
            return Objects.equals(id, ((TestClient) o).id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static PoolClientFactory<TestConfig, TestClient> newFactory() throws Exception {
        return new PoolClientFactory<>(new TestConfig("default"), TestClient.class.getName());
    }

    @Test
    void discoversConstructorAndLifecycleMethods() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        Assertions.assertNotNull(factory);
    }

    @Test
    void rejectsUnknownClientClass() {
        Assertions.assertThrows(ClassNotFoundException.class,
                () -> new PoolClientFactory<TestConfig, TestClient>(
                        new TestConfig("default"), "no.such.Class"));
    }

    @Test
    void rejectsClientWithoutConfigConstructorAndLifecycleMethods() {
        Assertions.assertThrows(NoSuchMethodException.class,
                () -> new PoolClientFactory<TestConfig, Object>(
                        new TestConfig("default"), "java.lang.String"));
    }

    @Test
    void configKeyManagement() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        Assertions.assertNull(factory.getConfig("k1"));
        Assertions.assertTrue(factory.configKeys().isEmpty());

        factory.configForKey("k1", new TestConfig("c1"));
        factory.configForKey("k2", new TestConfig("c2"));
        Assertions.assertEquals("c1", factory.getConfig("k1").id);
        Assertions.assertEquals("c2", factory.getConfig("k2").id);
        Assertions.assertEquals(2, factory.configKeys().size());
        Assertions.assertTrue(factory.configKeys().contains("k1"));

        TestConfig removed = factory.removeConfig("k1");
        Assertions.assertEquals("c1", removed.id);
        Assertions.assertNull(factory.getConfig("k1"));
        Assertions.assertEquals(1, factory.configKeys().size());
        Assertions.assertTrue(factory.configKeys().contains("k2"));
    }

    @Test
    void removeConfigReturnsNullForUnknownKey() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        Assertions.assertNull(factory.removeConfig("unknown"));
    }

    @Test
    void createUsesDefaultConfigWhenKeyHasNone() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        TestClient client = factory.create("some-key");
        Assertions.assertEquals("default", client.id);
    }

    @Test
    void createUsesKeyConfigWhenPresent() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        factory.configForKey("k1", new TestConfig("special"));
        Assertions.assertEquals("special", factory.create("k1").id);
        // a key without a dedicated config still falls back to the default
        Assertions.assertEquals("default", factory.create("k2").id);
    }

    @Test
    void createWrapsConstructorFailureIntoClientException() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory =
                new PoolClientFactory<>(new TestConfig("default", true), TestClient.class.getName());
        factory.configForKey("k1", new TestConfig("special", true));
        Assertions.assertThrows(MilvusClientException.class, () -> factory.create("k1"));
        Assertions.assertThrows(MilvusClientException.class, () -> factory.create("any"));
    }

    @Test
    void wrapReturnsDefaultPooledObject() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        TestClient client = new TestClient(new TestConfig("x"));
        PooledObject<TestClient> pooled = factory.wrap(client);
        Assertions.assertSame(client, pooled.getObject());
    }

    @Test
    void destroyObjectInvokesCloseMethod() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        TestClient client = factory.create("k");
        factory.destroyObject("k", factory.wrap(client));
        Assertions.assertEquals(1, client.closeCalls);
    }

    @Test
    void validateObjectInvokesClientIsReady() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        TestClient client = factory.create("k");
        Assertions.assertTrue(factory.validateObject("k", factory.wrap(client)));
        Assertions.assertEquals(1, client.readyCalls);

        client.ready = false;
        Assertions.assertFalse(factory.validateObject("k", factory.wrap(client)));
    }

    @Test
    void activateAndPassivateDelegateToBase() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory = newFactory();
        TestClient client = factory.create("k");
        PooledObject<TestClient> pooled = factory.wrap(client);
        factory.activateObject("k", pooled);
        factory.passivateObject("k", pooled);
    }

    @Test
    void exceptionCarriesClientErrorCode() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory =
                new PoolClientFactory<>(new TestConfig("default", true), TestClient.class.getName());
        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class,
                () -> factory.create("k"));
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR, ex.getErrorCode());
    }
}
