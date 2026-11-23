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

package io.milvus.unit.v2.common;

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.v2.common.CacheConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class CacheConfigTest {
    @Test
    void setsAndGetsCapacityOfBothCaches() {
        int original = SchemaCache.getCapacity();
        try {
            SchemaCache.getInstance().clear();
            CollectionTsCache.getInstance().clear();

            CacheConfig.setCapacity(24);

            assertEquals(24, CacheConfig.getCapacity());
            assertEquals(24, SchemaCache.getCapacity());
            assertEquals(24, CollectionTsCache.getCapacity());
        } finally {
            CacheConfig.setCapacity(original);
            SchemaCache.getInstance().clear();
            CollectionTsCache.getInstance().clear();
        }
    }

    @Test
    void rejectsNonPositiveCapacities() {
        assertThrows(IllegalArgumentException.class, () -> CacheConfig.setCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> CacheConfig.setCapacity(-1));
    }

    @Test
    void concurrentSetCapacityKeepsBothCachesInLockstep() throws Exception {
        int original = SchemaCache.getCapacity();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            SchemaCache.getInstance().clear();
            CollectionTsCache.getInstance().clear();

            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                final int capacity = 8 + i;
                futures.add(pool.submit(() -> {
                    awaitStart(start);
                    for (int j = 0; j < 200; j++) {
                        CacheConfig.setCapacity(capacity);
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

            assertEquals(SchemaCache.getCapacity(), CollectionTsCache.getCapacity());
        } finally {
            pool.shutdownNow();
            CacheConfig.setCapacity(original);
            SchemaCache.getInstance().clear();
            CollectionTsCache.getInstance().clear();
        }
    }

    private static void awaitStart(CountDownLatch start) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
