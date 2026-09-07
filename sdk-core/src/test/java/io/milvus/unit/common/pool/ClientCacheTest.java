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

import io.milvus.pool.ClientCache;
import io.milvus.pool.PoolClientFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Objects;

@Tag("unit")
class ClientCacheTest {

    public static class TestConfig {
        final String id;

        TestConfig(String id) {
            this.id = id;
        }
    }

    public static class TestClient {
        final String id;

        public TestClient(TestConfig config) {
            this.id = config.id;
        }

        public void close(long maxWaitSeconds) {
        }

        public boolean clientIsReady() {
            return true;
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

    public static class TestClientCache extends ClientCache<TestClient> {
        TestClientCache(String key, GenericKeyedObjectPool<String, TestClient> pool) {
            super(key, pool);
        }
    }

    private GenericKeyedObjectPool<String, TestClient> pool;
    private TestClientCache cache;

    @BeforeEach
    void setUp() throws Exception {
        PoolClientFactory<TestConfig, TestClient> factory =
                new PoolClientFactory<>(new TestConfig("default"), TestClient.class.getName());
        GenericKeyedObjectPoolConfig<TestClient> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setMaxTotalPerKey(5);
        poolConfig.setMinIdlePerKey(1);
        pool = new GenericKeyedObjectPool<>(factory, poolConfig);
        cache = new TestClientCache("key1", pool);
    }

    @AfterEach
    void tearDown() {
        cache.stopTimer();
        pool.close();
    }

    @Test
    void thresholdsArePinned() {
        Assertions.assertEquals(100, ClientCache.THRESHOLD_INCREASE);
        Assertions.assertEquals(50, ClientCache.THRESHOLD_DECREASE);
    }

    @Test
    void getClientBorrowsFromPoolAndCaches() {
        TestClient client = cache.getClient();
        Assertions.assertNotNull(client);
        Assertions.assertEquals("default", client.id);
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }

    @Test
    void getClientReusesCachedInstance() {
        TestClient first = cache.getClient();
        TestClient second = cache.getClient();
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }

    @Test
    void returnClientDecrementsButKeepsClientInCache() {
        TestClient client = cache.getClient();
        cache.returnClient(client);
        // The client is not returned to the pool immediately; the QPS timer retires it later.
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }

    @Test
    void returnUnknownClientIsNoOp() {
        TestClient client = cache.getClient();
        TestClient orphan = new TestClient(new TestConfig("orphan"));
        cache.returnClient(orphan);
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }

    @Test
    void fetchClientPerSecondStartsAtZero() {
        Assertions.assertEquals(0.0F, cache.fetchClientPerSecond());
    }

    @Test
    void preparePoolPrefetchesMinIdleClients() {
        cache.preparePool();
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }

    @Test
    void stopTimerReturnsClientsToPool() {
        cache.getClient();
        cache.stopTimer();
        Assertions.assertEquals(0, pool.getNumActive("key1"));
        Assertions.assertEquals(1, pool.getNumIdle("key1"));
    }

    @Test
    void getClientAfterStopTimerBorrowsAgain() {
        cache.getClient();
        cache.stopTimer();
        TestClient client = cache.getClient();
        Assertions.assertNotNull(client);
        Assertions.assertEquals(1, pool.getNumActive("key1"));
    }
}
