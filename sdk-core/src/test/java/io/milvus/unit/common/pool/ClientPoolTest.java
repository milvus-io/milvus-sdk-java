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

import io.milvus.pool.ClientPool;
import io.milvus.pool.PoolClientFactory;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Objects;

@Tag("unit")
class ClientPoolTest {

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

    public static class TestClientPool extends ClientPool<TestConfig, TestClient> {
        TestClientPool(PoolConfig config, PoolClientFactory<TestConfig, TestClient> factory) {
            super(config, factory);
        }
    }

    private static TestClientPool newPool() throws Exception {
        return new TestClientPool(PoolConfig.builder().build(),
                new PoolClientFactory<>(new TestConfig("default"), TestClient.class.getName()));
    }

    @Test
    void configDelegationToFactory() throws Exception {
        TestClientPool pool = newPool();
        pool.configForKey("k1", new TestConfig("c1"));
        Assertions.assertEquals("c1", pool.getConfig("k1").id);
        Assertions.assertTrue(pool.configKeys().contains("k1"));

        pool.removeConfig("k1");
        Assertions.assertNull(pool.getConfig("k1"));
        pool.close();
    }

    @Test
    void idleAndActiveCountsStartAtZero() throws Exception {
        TestClientPool pool = newPool();
        Assertions.assertEquals(0, pool.getIdleClientNumber("k"));
        Assertions.assertEquals(0, pool.getActiveClientNumber("k"));
        Assertions.assertEquals(0, pool.getTotalIdleClientNumber());
        Assertions.assertEquals(0, pool.getTotalActiveClientNumber());
        pool.close();
    }

    @Test
    void getClientBorrowsAndReturnClientWorks() throws Exception {
        TestClientPool pool = newPool();
        TestClient client = pool.getClient("k");
        Assertions.assertNotNull(client);
        Assertions.assertEquals("default", client.id);
        Assertions.assertEquals(1, pool.getActiveClientNumber("k"));

        pool.returnClient("k", client);
        pool.close();
    }

    @Test
    void getClientSameInstanceWhileCached() throws Exception {
        TestClientPool pool = newPool();
        TestClient first = pool.getClient("k");
        TestClient second = pool.getClient("k");
        Assertions.assertSame(first, second);
        pool.close();
    }

    @Test
    void getClientAfterCloseThrows() throws Exception {
        TestClientPool pool = newPool();
        pool.close();
        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class,
                () -> pool.getClient("k"));
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR, ex.getErrorCode());
    }

    @Test
    void returnClientWithUnknownKeyIsNoOp() throws Exception {
        TestClientPool pool = newPool();
        pool.returnClient("no-such-key", new TestClient(new TestConfig("orphan")));
        pool.close();
    }

    @Test
    void fetchClientPerSecondUnknownKeyIsZero() throws Exception {
        TestClientPool pool = newPool();
        Assertions.assertEquals(0.0F, pool.fetchClientPerSecond("nope"));
        pool.close();
    }

    @Test
    void preparePoolPrewarmsMinIdleClients() throws Exception {
        TestClientPool pool = new TestClientPool(PoolConfig.builder().minIdlePerKey(2).build(),
                new PoolClientFactory<>(new TestConfig("default"), TestClient.class.getName()));
        pool.preparePool("k");
        Assertions.assertEquals(2, pool.getActiveClientNumber("k"));
        TestClient client = pool.getClient("k");
        Assertions.assertNotNull(client);
        pool.close();
    }

    @Test
    void clearRemovesClients() throws Exception {
        TestClientPool pool = newPool();
        TestClient client = pool.getClient("k");
        pool.returnClient("k", client);
        pool.clear();
        Assertions.assertEquals(0, pool.getTotalActiveClientNumber());
        pool.close();
    }

    @Test
    void closeIsIdempotent() throws Exception {
        TestClientPool pool = newPool();
        pool.close();
        pool.close();
    }
}
