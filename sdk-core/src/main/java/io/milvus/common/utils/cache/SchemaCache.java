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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SchemaCache {
    public static final int DEFAULT_CAPACITY = 4096;

    @FunctionalInterface
    public interface Loader {
        DescribeCollectionResponse load();
    }

    @FunctionalInterface
    public interface AsyncLoader {
        CompletableFuture<DescribeCollectionResponse> load();
    }

    private static final SchemaCache INSTANCE = new SchemaCache();

    private static final class Entry {
        private DescribeCollectionResponse response;
        private final AtomicLong lastAccess;

        private Entry(DescribeCollectionResponse response, long lastAccess) {
            this.response = response;
            this.lastAccess = new AtomicLong(lastAccess);
        }
    }

    private static final class LoadState {
        private final AtomicBoolean invalidated = new AtomicBoolean(false);
        private boolean completed;
        private int waiters;
        private DescribeCollectionResponse response;
        private Throwable failure;
        private CompletableFuture<DescribeCollectionResponse> loaderFuture;
        private final CompletableFuture<DescribeCollectionResponse> future = new CompletableFuture<>();
    }

    private static final class LoadKey {
        private final CollectionCacheKey collectionKey;
        private final Object scope;

        private LoadKey(CollectionCacheKey collectionKey, Object scope) {
            this.collectionKey = collectionKey;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof LoadKey)) {
                return false;
            }
            LoadKey that = (LoadKey) object;
            return collectionKey.equals(that.collectionKey) && scope == that.scope;
        }

        @Override
        public int hashCode() {
            return 31 * collectionKey.hashCode() + System.identityHashCode(scope);
        }
    }

    private final int capacity;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong accessSequence = new AtomicLong();
    private final Map<CollectionCacheKey, Entry> cache = new HashMap<>();
    private final Object loadingLock = new Object();
    private final Map<LoadKey, LoadState> loading = new HashMap<>();

    public SchemaCache() {
        this(DEFAULT_CAPACITY);
    }

    public SchemaCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Cache capacity must be greater than zero");
        }
        this.capacity = capacity;
    }

    public static SchemaCache getInstance() {
        return INSTANCE;
    }

    /**
     * Loads a schema while coalescing concurrent loads only within the same identity-based scope.
     * Completed schemas remain shared by endpoint, database, and collection. A client should pass
     * a stable per-client scope so another client's credentials or RPC deadline cannot control its
     * in-flight load.
     */
    public DescribeCollectionResponse getOrLoad(String endpoint, String databaseName, String collectionName,
                                                boolean forceUpdate, Object loadScope, Loader loader) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        DescribeCollectionResponse initial = getCached(key);
        if (initial != null && !forceUpdate) {
            return initial;
        }

        LoadKey loadKey = new LoadKey(key, Objects.requireNonNull(loadScope, "loadScope cannot be null"));
        LoadState newState = new LoadState();
        LoadState state;
        synchronized (loadingLock) {
            state = loading.get(loadKey);
            if (state == null) {
                retainWaiter(newState);
                loading.put(loadKey, newState);
            }
        }
        if (state != null) {
            return await(loadKey, state);
        }
        state = newState;

        try {
            DescribeCollectionResponse current = getCached(key);
            if (current != null && (!forceUpdate || current != initial)) {
                publishLoadResult(loadKey, state, current, null);
                return current;
            }

            DescribeCollectionResponse loaded = loader.load();
            setCachedIfValid(key, loaded, state);
            publishLoadResult(loadKey, state, loaded, null);
            return loaded;
        } catch (Throwable throwable) {
            publishLoadResult(loadKey, state, null, throwable);
            throw propagate(throwable);
        } finally {
            releaseWaiter(loadKey, state, false);
            removeLoad(loadKey, state);
        }
    }

    /**
     * Asynchronously loads a schema with the same cache, scope, coalescing, and invalidation
     * semantics as {@link #getOrLoad(String, String, String, boolean, Object, Loader)}.
     * Cancelling one returned future does not cancel a load shared by other callers; cancelling
     * the final waiter cancels the underlying loader.
     */
    public CompletableFuture<DescribeCollectionResponse> getOrLoadAsync(
            String endpoint, String databaseName, String collectionName,
            boolean forceUpdate, Object loadScope, AsyncLoader loader) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        DescribeCollectionResponse initial = getCached(key);
        if (initial != null && !forceUpdate) {
            return CompletableFuture.completedFuture(initial);
        }

        LoadKey loadKey = new LoadKey(key, Objects.requireNonNull(loadScope, "loadScope cannot be null"));
        LoadState newState = new LoadState();
        LoadState state;
        synchronized (loadingLock) {
            state = loading.get(loadKey);
            if (state == null) {
                retainWaiter(newState);
                loading.put(loadKey, newState);
            }
        }
        if (state != null) {
            return dependentFuture(loadKey, state, false);
        }
        state = newState;
        CompletableFuture<DescribeCollectionResponse> dependent = dependentFuture(loadKey, state, true);

        DescribeCollectionResponse current = getCached(key);
        if (current != null && (!forceUpdate || current != initial)) {
            publishLoadResult(loadKey, state, current, null);
            return dependent;
        }

        CompletableFuture<DescribeCollectionResponse> loadFuture;
        try {
            loadFuture = Objects.requireNonNull(loader, "loader cannot be null").load();
            if (loadFuture == null) {
                throw new NullPointerException("Async schema loader returned null future");
            }
        } catch (Throwable throwable) {
            publishLoadResult(loadKey, state, null, throwable);
            return dependent;
        }

        LoadState finalState = state;
        registerLoader(state, loadFuture);
        loadFuture.whenComplete((loaded, throwable) -> {
            Throwable failure = throwable == null ? null : unwrapCompletionThrowable(throwable);
            if (failure == null) {
                try {
                    setCachedIfValid(key, loaded, finalState);
                } catch (Throwable completionFailure) {
                    failure = completionFailure;
                }
            }
            publishLoadResult(loadKey, finalState, failure == null ? loaded : null, failure);
        });
        return dependent;
    }

    public DescribeCollectionResponse get(String endpoint, String databaseName, String collectionName) {
        return getCached(CollectionCacheKey.create(endpoint, databaseName, collectionName));
    }

    public void set(String endpoint, String databaseName, String collectionName,
                    DescribeCollectionResponse response) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        invalidateLoad(key);
        setCached(key, response);
    }

    public void invalidate(String endpoint, String databaseName, String collectionName) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        invalidateLoad(key);
        lock.writeLock().lock();
        try {
            cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void invalidateDb(String endpoint, String databaseName) {
        CollectionCacheKey prefix = CollectionCacheKey.create(endpoint, databaseName, "");
        synchronized (loadingLock) {
            loading.entrySet().removeIf(entry -> {
                if (!sameDatabase(entry.getKey().collectionKey, prefix)) {
                    return false;
                }
                entry.getValue().invalidated.set(true);
                return true;
            });
        }
        lock.writeLock().lock();
        try {
            cache.keySet().removeIf(key -> sameDatabase(key, prefix));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        synchronized (loadingLock) {
            loading.values().forEach(state -> state.invalidated.set(true));
            loading.clear();
        }
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private DescribeCollectionResponse getCached(CollectionCacheKey key) {
        lock.readLock().lock();
        try {
            Entry entry = cache.get(key);
            if (entry == null) {
                return null;
            }
            touch(entry);
            return entry.response;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void setCached(CollectionCacheKey key, DescribeCollectionResponse response) {
        lock.writeLock().lock();
        try {
            setCacheNoLocked(key, response);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void setCachedIfValid(CollectionCacheKey key, DescribeCollectionResponse response, LoadState state) {
        lock.writeLock().lock();
        try {
            if (!state.invalidated.get()) {
                setCacheNoLocked(key, response);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void setCacheNoLocked(CollectionCacheKey key, DescribeCollectionResponse response) {
        Entry entry = cache.get(key);
        if (entry != null) {
            entry.response = response;
            touch(entry);
        } else {
            cache.put(key, new Entry(response, nextAccess()));
            evictIfNeeded();
        }
    }

    private void invalidateLoad(CollectionCacheKey key) {
        synchronized (loadingLock) {
            loading.entrySet().removeIf(entry -> {
                if (!entry.getKey().collectionKey.equals(key)) {
                    return false;
                }
                entry.getValue().invalidated.set(true);
                return true;
            });
        }
    }

    private DescribeCollectionResponse await(LoadKey loadKey, LoadState state) {
        retainWaiter(state);
        try {
            synchronized (state) {
                while (!state.completed) {
                    try {
                        state.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for collection schema load", e);
                    }
                }
            }
            if (state.failure != null) {
                throw propagate(state.failure);
            }
            return state.response;
        } finally {
            releaseWaiter(loadKey, state, false);
        }
    }

    private boolean markCompleted(LoadState state, DescribeCollectionResponse response, Throwable failure) {
        synchronized (state) {
            if (state.completed) {
                return false;
            }
            state.response = response;
            state.failure = failure;
            state.completed = true;
            state.notifyAll();
            return true;
        }
    }

    private void completeFuture(LoadState state, DescribeCollectionResponse response, Throwable failure) {
        if (failure == null) {
            state.future.complete(response);
        } else {
            state.future.completeExceptionally(failure);
        }
    }

    private CompletableFuture<DescribeCollectionResponse> dependentFuture(
            LoadKey loadKey, LoadState state, boolean waiterAlreadyRetained) {
        if (!waiterAlreadyRetained) {
            retainWaiter(state);
        }
        AtomicBoolean released = new AtomicBoolean(false);
        CompletableFuture<DescribeCollectionResponse> dependent =
                new CompletableFuture<DescribeCollectionResponse>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        boolean cancelled = super.cancel(mayInterruptIfRunning);
                        if (cancelled && released.compareAndSet(false, true)) {
                            releaseWaiter(loadKey, state, true);
                        }
                        return cancelled;
                    }
                };
        state.future.whenComplete((response, throwable) -> {
            if (!dependent.isDone()) {
                if (throwable == null) {
                    dependent.complete(response);
                } else {
                    dependent.completeExceptionally(unwrapCompletionThrowable(throwable));
                }
            }
            if (released.compareAndSet(false, true)) {
                releaseWaiter(loadKey, state, false);
            }
        });
        return dependent;
    }

    private void retainWaiter(LoadState state) {
        synchronized (state) {
            state.waiters++;
        }
    }

    private void registerLoader(LoadState state,
                                CompletableFuture<DescribeCollectionResponse> loaderFuture) {
        boolean cancelLoader;
        synchronized (state) {
            state.loaderFuture = loaderFuture;
            cancelLoader = state.waiters == 0 && state.invalidated.get() && !state.completed;
        }
        if (cancelLoader) {
            loaderFuture.cancel(true);
        }
    }

    private void releaseWaiter(LoadKey loadKey, LoadState state, boolean cancelled) {
        CompletableFuture<DescribeCollectionResponse> loaderFuture = null;
        boolean cancelLoad = false;
        synchronized (state) {
            state.waiters--;
            if (cancelled && state.waiters == 0 && !state.completed) {
                state.invalidated.set(true);
                loaderFuture = state.loaderFuture;
                cancelLoad = true;
            }
        }
        if (cancelLoad) {
            removeLoad(loadKey, state);
            if (loaderFuture != null) {
                loaderFuture.cancel(true);
            }
        }
    }

    private void publishLoadResult(LoadKey loadKey, LoadState state,
                                   DescribeCollectionResponse response, Throwable failure) {
        if (!markCompleted(state, response, failure)) {
            return;
        }
        // Keep the completed state discoverable until its result is published to synchronous
        // waiters, then remove it before CompletableFuture continuations can execute inline.
        removeLoad(loadKey, state);
        completeFuture(state, response, failure);
    }

    private void removeLoad(LoadKey loadKey, LoadState state) {
        synchronized (loadingLock) {
            if (loading.get(loadKey) == state) {
                loading.remove(loadKey);
            }
        }
    }

    private Throwable unwrapCompletionThrowable(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private RuntimeException propagate(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        return new RuntimeException(throwable);
    }

    private boolean sameDatabase(CollectionCacheKey key, CollectionCacheKey prefix) {
        return key.getEndpoint().equals(prefix.getEndpoint())
                && key.getDatabaseName().equals(prefix.getDatabaseName());
    }

    private long nextAccess() {
        return accessSequence.incrementAndGet();
    }

    private void touch(Entry entry) {
        entry.lastAccess.accumulateAndGet(nextAccess(), Math::max);
    }

    private void evictIfNeeded() {
        if (cache.size() <= capacity) {
            return;
        }
        cache.entrySet().stream()
                .min(Comparator.comparingLong(item -> item.getValue().lastAccess.get()))
                .map(Map.Entry::getKey)
                .ifPresent(cache::remove);
    }
}
