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

package io.milvus.common.utils.cache;

import io.milvus.grpc.DescribeCollectionResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCacheTest {
    private final Object loadScope = new Object();

    @Test
    void isolatesEntriesAndSupportsForceRefresh() {
        SchemaCache cache = new SchemaCache();
        DescribeCollectionResponse first = response(1L);
        DescribeCollectionResponse refreshed = response(2L);
        AtomicInteger loads = new AtomicInteger();

        assertSame(first, cache.getOrLoad("host:19530", null, "coll", false, loadScope, () -> {
            loads.incrementAndGet();
            return first;
        }));
        assertSame(first, cache.getOrLoad("HOST:19530", "default", "coll", false, loadScope,
                () -> response(99L)));
        assertSame(refreshed, cache.getOrLoad("host:19530", "default", "coll", true, loadScope, () -> {
            loads.incrementAndGet();
            return refreshed;
        }));
        cache.set("host:19531", "default", "coll", response(3L));

        assertEquals(2, loads.get());
        assertSame(refreshed, cache.get("host:19530", "default", "coll"));
        assertEquals(3L, cache.get("host:19531", "default", "coll").getCollectionID());
    }

    @Test
    void sharesOneInflightLoadAcrossThreads() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        List<Future<DescribeCollectionResponse>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> cache.getOrLoad(
                        "host:19530", "db", "coll", false, loadScope, () -> {
                            loads.incrementAndGet();
                            loaderStarted.countDown();
                            await(releaseLoader);
                            return response(10L);
                        })));
            }
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
            releaseLoader.countDown();
            for (Future<DescribeCollectionResponse> future : futures) {
                assertEquals(10L, future.get(5, TimeUnit.SECONDS).getCollectionID());
            }
            assertEquals(1, loads.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void differentLoadScopesUseTheirOwnLoaderPolicy() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        Object noDeadlineClient = new Object();
        Object deadlineClient = new Object();
        AtomicInteger loads = new AtomicInteger();

        try {
            Future<DescribeCollectionResponse> loaderFuture = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, noDeadlineClient, () -> {
                        loads.incrementAndGet();
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return response(10L);
                    }));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));

            assertThrows(IllegalStateException.class, () ->
                    cache.getOrLoad("host:19530", "db", "coll", false, deadlineClient, () -> {
                        loads.incrementAndGet();
                        throw new IllegalStateException("deadline exceeded");
                    }));
            assertEquals(2, loads.get());
            assertFalse(loaderFuture.isDone());

            releaseLoader.countDown();
            assertEquals(10L, loaderFuture.get(5, TimeUnit.SECONDS).getCollectionID());
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void invalidationDuringLoadPreventsRepopulation() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);

        try {
            Future<DescribeCollectionResponse> future = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return response(11L);
                    }));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
            cache.invalidate("host:19530", "db", "coll");
            releaseLoader.countDown();

            assertEquals(11L, future.get(5, TimeUnit.SECONDS).getCollectionID());
            assertNull(cache.get("host:19530", "db", "coll"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void invalidationAndSchemaPublicationAreAtomic() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicReference<Thread> loaderThread = new AtomicReference<>();

        java.lang.reflect.Field lockField = SchemaCache.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        ReentrantReadWriteLock cacheLock = (ReentrantReadWriteLock) lockField.get(cache);

        try {
            Future<DescribeCollectionResponse> future = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loaderThread.set(Thread.currentThread());
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return response(13L);
                    }));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));

            cacheLock.writeLock().lock();
            try {
                releaseLoader.countDown();
                awaitQueued(cacheLock, loaderThread.get());
                cache.invalidate("host:19530", "db", "coll");
            } finally {
                cacheLock.writeLock().unlock();
            }

            assertEquals(13L, future.get(5, TimeUnit.SECONDS).getCollectionID());
            assertNull(cache.get("host:19530", "db", "coll"));
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void callerAfterInvalidationStartsNewLoadGeneration() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch oldLoaderStarted = new CountDownLatch(1);
        CountDownLatch releaseOldLoader = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();

        try {
            Future<DescribeCollectionResponse> oldFuture = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        oldLoaderStarted.countDown();
                        await(releaseOldLoader);
                        return response(11L);
                    }));
            assertTrue(oldLoaderStarted.await(5, TimeUnit.SECONDS));

            cache.invalidate("host:19530", "db", "coll");
            Future<DescribeCollectionResponse> newFuture = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        return response(22L);
                    }));

            assertEquals(22L, newFuture.get(5, TimeUnit.SECONDS).getCollectionID());
            releaseOldLoader.countDown();
            assertEquals(11L, oldFuture.get(5, TimeUnit.SECONDS).getCollectionID());
            assertEquals(2, loads.get());
            assertEquals(22L, cache.get("host:19530", "db", "coll").getCollectionID());
        } finally {
            releaseOldLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void interruptedWaiterExitsWithoutCancellingSharedLoad() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        CountDownLatch waiterStarted = new CountDownLatch(1);
        CountDownLatch waiterExited = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        AtomicBoolean interruptRestored = new AtomicBoolean(false);
        AtomicReference<Throwable> waiterFailure = new AtomicReference<>();

        try {
            Future<DescribeCollectionResponse> loaderFuture = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return response(30L);
                    }));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));

            Thread waiter = new Thread(() -> {
                waiterStarted.countDown();
                try {
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        return response(31L);
                    });
                } catch (Throwable throwable) {
                    waiterFailure.set(throwable);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                } finally {
                    waiterExited.countDown();
                }
            });
            waiter.start();
            assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));

            waiter.interrupt();
            assertTrue(waiterExited.await(5, TimeUnit.SECONDS));
            waiter.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(waiter.isAlive());
            assertTrue(interruptRestored.get());
            assertTrue(waiterFailure.get() instanceof RuntimeException);
            assertTrue(waiterFailure.get().getMessage().contains("Interrupted while waiting"));
            assertEquals(1, loads.get());
            assertFalse(loaderFuture.isDone());

            releaseLoader.countDown();
            assertEquals(30L, loaderFuture.get(5, TimeUnit.SECONDS).getCollectionID());
            assertEquals(30L, cache.get("host:19530", "db", "coll").getCollectionID());
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void unrelatedInvalidationDoesNotSuppressInflightLoad() throws Exception {
        SchemaCache cache = new SchemaCache();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);

        try {
            Future<DescribeCollectionResponse> future = executor.submit(() ->
                    cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return response(12L);
                    }));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
            cache.invalidate("host:19530", "db", "other");
            releaseLoader.countDown();

            assertEquals(12L, future.get(5, TimeUnit.SECONDS).getCollectionID());
            assertEquals(12L, cache.get("host:19530", "db", "coll").getCollectionID());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void supportsDatabaseInvalidationAndLruEviction() {
        SchemaCache cache = new SchemaCache(2);
        cache.set("host:19530", "db", "one", response(1L));
        cache.set("host:19530", "db", "two", response(2L));
        cache.get("host:19530", "db", "one");
        cache.set("host:19530", "other", "three", response(3L));

        assertNull(cache.get("host:19530", "db", "two"));
        cache.invalidateDb("host:19530", "db");
        assertNull(cache.get("host:19530", "db", "one"));
        assertEquals(3L, cache.get("host:19530", "other", "three").getCollectionID());
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void failedLoadIsNotCachedAndCanBeRetried() {
        SchemaCache cache = new SchemaCache();
        AtomicInteger loads = new AtomicInteger();

        assertThrows(IllegalStateException.class, () ->
                cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    throw new IllegalStateException("load failed");
                }));
        DescribeCollectionResponse loaded = cache.getOrLoad(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return response(4L);
                });

        assertEquals(2, loads.get());
        assertEquals(4L, loaded.getCollectionID());
    }

    private static DescribeCollectionResponse response(long collectionId) {
        return DescribeCollectionResponse.newBuilder().setCollectionID(collectionId).build();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void awaitQueued(ReentrantReadWriteLock lock, Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!lock.hasQueuedThread(thread)) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Loader did not reach the schema publication boundary");
            }
            Thread.yield();
        }
    }
}
