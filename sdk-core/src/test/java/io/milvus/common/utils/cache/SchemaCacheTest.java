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
import java.util.concurrent.CompletableFuture;
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
    void sharesOneInflightAsyncLoadWithoutBlockingCallers() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();

        CompletableFuture<DescribeCollectionResponse> first = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return loaderFuture;
                });
        CompletableFuture<DescribeCollectionResponse> second = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return CompletableFuture.completedFuture(response(99L));
                });

        assertEquals(1, loads.get());
        assertFalse(first.isDone());
        assertFalse(second.isDone());

        loaderFuture.complete(response(10L));
        assertEquals(10L, first.get(5, TimeUnit.SECONDS).getCollectionID());
        assertEquals(10L, second.get(5, TimeUnit.SECONDS).getCollectionID());
        assertEquals(10L, cache.get("host:19530", "db", "coll").getCollectionID());
    }

    @Test
    void marksLoadCompletedBeforeRemovingInflightGeneration() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();
        CompletableFuture<DescribeCollectionResponse> result = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> loaderFuture);

        java.lang.reflect.Field loadingLockField = SchemaCache.class.getDeclaredField("loadingLock");
        loadingLockField.setAccessible(true);
        Object loadingLock = loadingLockField.get(cache);
        java.lang.reflect.Field loadingField = SchemaCache.class.getDeclaredField("loading");
        loadingField.setAccessible(true);
        Thread completer;

        synchronized (loadingLock) {
            java.util.Map<?, ?> loading = (java.util.Map<?, ?>) loadingField.get(cache);
            Object state = loading.values().iterator().next();
            completer = new Thread(() -> loaderFuture.complete(response(15L)));
            completer.start();

            awaitCompleted(state);
            assertEquals(1, loading.size());
            assertFalse(result.isDone());
        }

        completer.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(completer.isAlive());
        assertEquals(15L, result.get(5, TimeUnit.SECONDS).getCollectionID());
    }

    @Test
    void asyncInvalidationPreventsOldLoadFromRepopulatingCache() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> oldLoader = new CompletableFuture<>();

        CompletableFuture<DescribeCollectionResponse> oldResult = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> oldLoader);
        cache.invalidate("host:19530", "db", "coll");
        CompletableFuture<DescribeCollectionResponse> newResult = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope,
                () -> CompletableFuture.completedFuture(response(22L)));

        assertEquals(22L, newResult.get(5, TimeUnit.SECONDS).getCollectionID());
        oldLoader.complete(response(11L));
        assertEquals(11L, oldResult.get(5, TimeUnit.SECONDS).getCollectionID());
        assertEquals(22L, cache.get("host:19530", "db", "coll").getCollectionID());
    }

    @Test
    void cancellingOnlyAsyncWaiterCancelsSharedLoad() {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();

        CompletableFuture<DescribeCollectionResponse> result = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> loaderFuture);

        assertTrue(result.cancel(true));
        assertTrue(loaderFuture.isCancelled());
        assertNull(cache.get("host:19530", "db", "coll"));
    }

    @Test
    void lateLoaderRegistrationCancelsAfterLastWaiterLeaves() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();
        CompletableFuture<DescribeCollectionResponse> result = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> loaderFuture);

        Object state = getOnlyLoadState(cache);
        java.lang.reflect.Field loaderFutureField = state.getClass().getDeclaredField("loaderFuture");
        loaderFutureField.setAccessible(true);
        synchronized (state) {
            loaderFutureField.set(state, null);
        }

        assertTrue(result.cancel(true));
        assertFalse(loaderFuture.isCancelled());

        java.lang.reflect.Method registerLoader = SchemaCache.class.getDeclaredMethod(
                "registerLoader", state.getClass(), CompletableFuture.class);
        registerLoader.setAccessible(true);
        registerLoader.invoke(cache, state, loaderFuture);

        assertTrue(loaderFuture.isCancelled());
    }

    @Test
    void cancellingAsyncWaiterDoesNotCancelSharedLoad() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();

        CompletableFuture<DescribeCollectionResponse> cancelled = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> loaderFuture);
        CompletableFuture<DescribeCollectionResponse> remaining = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope,
                () -> CompletableFuture.completedFuture(response(99L)));

        assertTrue(cancelled.cancel(true));
        assertFalse(loaderFuture.isCancelled());
        loaderFuture.complete(response(12L));
        assertEquals(12L, remaining.get(5, TimeUnit.SECONDS).getCollectionID());
    }

    @Test
    void synchronousAndAsyncCallersShareInflightLoad() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<DescribeCollectionResponse> syncResponse = new AtomicReference<>();

        CompletableFuture<DescribeCollectionResponse> asyncResult = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return loaderFuture;
                });
        Thread syncCaller = new Thread(() -> syncResponse.set(cache.getOrLoad(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return response(99L);
                })));
        syncCaller.start();
        awaitWaiting(syncCaller);

        try {
            loaderFuture.complete(response(14L));
            assertEquals(14L, asyncResult.get(5, TimeUnit.SECONDS).getCollectionID());
            syncCaller.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(syncCaller.isAlive());
            assertEquals(14L, syncResponse.get().getCollectionID());
            assertEquals(1, loads.get());
        } finally {
            loaderFuture.complete(response(14L));
            syncCaller.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void inlineRetryAfterAsyncFailureStartsNewLoadGeneration() throws Exception {
        SchemaCache cache = new SchemaCache();
        CompletableFuture<DescribeCollectionResponse> loaderFuture = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<CompletableFuture<DescribeCollectionResponse>> retryFuture = new AtomicReference<>();

        CompletableFuture<DescribeCollectionResponse> first = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    return loaderFuture;
                });
        CompletableFuture<Void> continuation = first.handle((ignoredResponse, failure) -> {
            retryFuture.set(cache.getOrLoadAsync(
                    "host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        return CompletableFuture.completedFuture(response(20L));
                    }));
            return null;
        });

        loaderFuture.completeExceptionally(new IllegalStateException("load failed"));
        continuation.get(5, TimeUnit.SECONDS);

        assertEquals(2, loads.get());
        assertEquals(20L, retryFuture.get().get(5, TimeUnit.SECONDS).getCollectionID());
    }

    @Test
    void inlineRetryAfterSynchronousFailureStartsNewLoadGeneration() throws Exception {
        SchemaCache cache = new SchemaCache();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<Throwable> synchronousFailure = new AtomicReference<>();
        AtomicReference<CompletableFuture<DescribeCollectionResponse>> retryFuture = new AtomicReference<>();

        Thread synchronousLoader = new Thread(() -> {
            try {
                cache.getOrLoad("host:19530", "db", "coll", false, loadScope, () -> {
                    loads.incrementAndGet();
                    loaderStarted.countDown();
                    await(releaseLoader);
                    throw new IllegalStateException("load failed");
                });
            } catch (Throwable throwable) {
                synchronousFailure.set(throwable);
            }
        });
        synchronousLoader.start();
        assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));

        CompletableFuture<DescribeCollectionResponse> waiter = cache.getOrLoadAsync(
                "host:19530", "db", "coll", false, loadScope,
                () -> CompletableFuture.completedFuture(response(99L)));
        CompletableFuture<Void> continuation = waiter.handle((ignoredResponse, failure) -> {
            retryFuture.set(cache.getOrLoadAsync(
                    "host:19530", "db", "coll", false, loadScope, () -> {
                        loads.incrementAndGet();
                        return CompletableFuture.completedFuture(response(21L));
                    }));
            return null;
        });

        try {
            releaseLoader.countDown();
            continuation.get(5, TimeUnit.SECONDS);
            synchronousLoader.join(TimeUnit.SECONDS.toMillis(5));

            assertFalse(synchronousLoader.isAlive());
            assertTrue(synchronousFailure.get() instanceof IllegalStateException);
            assertEquals(2, loads.get());
            assertEquals(21L, retryFuture.get().get(5, TimeUnit.SECONDS).getCollectionID());
        } finally {
            releaseLoader.countDown();
            synchronousLoader.join(TimeUnit.SECONDS.toMillis(5));
        }
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

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Synchronous caller did not wait for the async schema load");
            }
            Thread.yield();
        }
    }

    private static void awaitCompleted(Object state) throws Exception {
        java.lang.reflect.Field completedField = state.getClass().getDeclaredField("completed");
        completedField.setAccessible(true);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            synchronized (state) {
                if (completedField.getBoolean(state)) {
                    return;
                }
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Load state was not completed before removal");
            }
            Thread.yield();
        }
    }

    private static Object getOnlyLoadState(SchemaCache cache) throws Exception {
        java.lang.reflect.Field loadingField = SchemaCache.class.getDeclaredField("loading");
        loadingField.setAccessible(true);
        java.util.Map<?, ?> loading = (java.util.Map<?, ?>) loadingField.get(cache);
        return loading.values().iterator().next();
    }
}
