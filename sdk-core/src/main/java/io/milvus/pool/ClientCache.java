package io.milvus.pool;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClientCache<T> {
    public static final int THRESHOLD_INCREASE = 100;
    public static final int THRESHOLD_DECREASE = 50;

    private static final Logger logger = LoggerFactory.getLogger(ClientCache.class);
    private final String key;
    private final GenericKeyedObjectPool<String, T> clientPool;
    private final CopyOnWriteArrayList<ClientWrapper<T>> activeClientList = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ClientWrapper<T>> retireClientList = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicLong totalCallNumber = new AtomicLong(0L);
    private final Lock clientListLock;
    private long lastCheckMs = 0L;
    private float fetchClientPerSecond = 0.0F;

    protected ClientCache(String key, GenericKeyedObjectPool<String, T> pool) {
        this.key = key;
        this.clientPool = pool;
        this.clientListLock = new ReentrantLock(true);

        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY); // set the highest priority for the timer
                return t;
            }
        };
        this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);

        startTimer(1000L);
    }

    public void preparePool() {
        try {
            // preparePool() will create minIdlePerKey MilvusClient objects in advance, put the pre-created clients
            // into activeClientList
            clientPool.preparePool(this.key);
            int minIdlePerKey = clientPool.getMinIdlePerKey();
            for (int i = 0; i < minIdlePerKey; i++) {
                activeClientList.add(new ClientWrapper<>(clientPool.borrowObject(this.key)));
            }

            if (logger.isDebugEnabled()) {
                logger.debug("ClientCache key: {} cache clients: {} ", key, activeClientList.size());
                logger.debug("Pool initialize idle: {} active: {} ", clientPool.getNumIdle(key), clientPool.getNumActive(key));
            }
//            System.out.printf("Key: %s, cache client: %d%n", key, activeClientList.size());
//            System.out.printf("Pool idle %d, active %d%n", clientPool.getNumIdle(key), clientPool.getNumActive(key));
        } catch (Exception e) {
            logger.error("Failed to prepare pool {}, exception: ", key, e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
    }

    // this method is called in an interval, it does the following tasks:
    // - if QPS is high, borrow client from the pool and put into activeClientList
    // - if QPS is low, pick a client from activeClientList and put into retireClientList
    //
    // Most of gRPC implementations uses a single long-lived HTTP/2 connection, each HTTP/2 connections have a limit
    // on the number of concurrent streams which is default 100. When the number of active RPCs on the connection
    // reaches this limit, additional RPCs are queued in the client and must wait for active RPCs to finish
    // before they are sent.
    //
    // Treat qps >= THRESHOLD_INCREASE as high, qps <= THRESHOLD_DECREASE as low
    private void checkQPS() {
        if (activeClientList.isEmpty()) {
            // reset the last check time point
            lastCheckMs = System.currentTimeMillis();
            return;
        }

        long totalCallNum = totalCallNumber.get();
        float perClientCall = (float) totalCallNum / activeClientList.size();
        long timeGapMs = System.currentTimeMillis() - lastCheckMs;
        if (timeGapMs == 0) {
            timeGapMs = 1;  // avoid zero
        }
        float perClientPerSecond = perClientCall * 1000 / timeGapMs;
        this.fetchClientPerSecond = (float) (totalCallNum * 1000) / timeGapMs;
        if (logger.isDebugEnabled()) {

            logger.debug("ClientCache key: {} fetchClientPerSecond: {} perClientPerSecond: {}, cached clients: {}",
                    key, fetchClientPerSecond, perClientPerSecond, activeClientList.size());
            logger.debug("Pool idle: {} active: {} ", clientPool.getNumIdle(key), clientPool.getNumActive(key));
        }
//        System.out.printf("Key: %s, fetchClientPerSecond: %.2f, perClientPerSecond: %.2f, cache client: %d%n", key, fetchClientPerSecond, perClientPerSecond, activeClientList.size());
//        System.out.printf("Pool idle %d, active %d%n", clientPool.getNumIdle(key), clientPool.getNumActive(key));

        // reset the counter and the last check time point
        totalCallNumber.set(0L);
        lastCheckMs = System.currentTimeMillis();

        if (perClientPerSecond >= THRESHOLD_INCREASE) {
            // try to create more clients to reduce the perClientPerSecond to under THRESHOLD_INCREASE
            // add no more than 3 clients since the qps could change during we're adding new clients
            // the next call of checkQPS() will add more clients if the perClientPerSecond is still high
            int expectedNum = (int) Math.ceil((double) totalCallNum / THRESHOLD_INCREASE);
            int moreNum = expectedNum - activeClientList.size();
            if (moreNum > 3) {
                moreNum = 3;
            }

            for (int k = 0; k < moreNum; k++) {
                T client = fetchFromPool();
                // if the pool reaches MaxTotalPerKey, the new client is null
                if (client == null) {
                    break;
                }

                ClientWrapper<T> wrapper = new ClientWrapper<>(client);
                activeClientList.add(wrapper);

                if (logger.isDebugEnabled()) {
                    logger.debug("ClientCache key: {} borrows a client", key);
                }
//                System.out.printf("Key: %s borrows a client%n", key);
            }
        }

        if (activeClientList.size() > 1 && perClientPerSecond <= THRESHOLD_DECREASE) {
            // if activeClientList has only one client, no need to retire it
            // otherwise, retire the max load client
            int maxLoad = -1000;
            int maxIndex = -1;
            for (int i = 0; i < activeClientList.size(); i++) {
                ClientWrapper<T> wrapper = activeClientList.get(i);
                int refCount = wrapper.getRefCount();
                if (refCount > maxLoad) {
                    maxLoad = refCount;
                    maxIndex = i;
                }
            }
            if (maxIndex >= 0) {
                ClientWrapper<T> wrapper = activeClientList.get(maxIndex);
                activeClientList.remove(maxIndex);
                retireClientList.add(wrapper);
            }
        }

        // return the retired client to pool if ref count is zero
        returnRetiredClients();
    }

    private void returnRetiredClients() {
        retireClientList.removeIf(wrapper -> {
            if (wrapper.getRefCount() <= 0) {
                returnToPool(wrapper.getClient());

                if (logger.isDebugEnabled()) {
                    logger.debug("ClientCache key: {} returns a client", key);
                }
//                System.out.printf("Key: %s returns a client%n", key);
                return true;
            }
            return false;
        });
    }

    private void startTimer(long interval) {
        if (interval < 1000L) {
            interval = 1000L; // min 1000
        }

        lastCheckMs = System.currentTimeMillis();
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                checkQPS();
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    public void stopTimer() {
        scheduler.shutdown();
    }

    public T getClient() {
        if (activeClientList.isEmpty()) {
            // multiple threads can run into this section, add a lock to ensure only one thread can fetch the first
            // client object, this section is entered only one time, the lock doesn't affect major performance
            clientListLock.lock();
            try {
                if (activeClientList.isEmpty()) {
                    T client = fetchFromPool();
                    if (client == null) {
                        // no need to count the totalCallNumber is cannot fetch a client
                        return null; // reach MaxTotalPerKey?
                    }
                    ClientWrapper<T> wrapper = new ClientWrapper<>(client);
                    activeClientList.add(wrapper);
                    totalCallNumber.incrementAndGet(); // count the totalCallNumber when successfully fetch a client
                    return wrapper.getClient();
                }
            } finally {
                clientListLock.unlock();
            }
        }

        // round-robin is not a good choice because the activeClientList is occasionally changed.
        // here we return the minimum load client, the for loop of CopyOnWriteArrayList is high performance
        // typically, the activeClientList is not a large list since a dozen of clients can take thousands of qps,
        // I suppose the loop is a cheap operation.
        int minLoad = Integer.MAX_VALUE;
        ClientWrapper<T> wrapper = null;
        for (ClientWrapper<T> tempWrapper : activeClientList) {
            if (tempWrapper.getRefCount() < minLoad) {
                minLoad = tempWrapper.getRefCount();
                wrapper = tempWrapper;
            }
        }
        if (wrapper == null) {
            // should not be here, the "if (activeClientList.isEmpty())" section has already ensured
            // there must be a client in activeClientList
            wrapper = activeClientList.get(0);
        }

        totalCallNumber.incrementAndGet(); // count the totalCallNumber when successfully fetch a client
        return wrapper.getClient();
    }

    public void returnClient(T grpcClient) {
        // for-loop of CopyOnWriteArrayList is thread safe
        // this method only decrements the call number, the checkQPS timer will retire client accordingly
        for (ClientWrapper<T> wrapper : activeClientList) {
            if (wrapper.equals(grpcClient)) {
                wrapper.returnClient();
                return;
            }
        }
        for (ClientWrapper<T> wrapper : retireClientList) {
            if (wrapper.equals(grpcClient)) {
                wrapper.returnClient();
                return;
            }
        }
    }

    private T fetchFromPool() {
        try {
            // borrowed clients exceeds MaxTotalPerKey?
            if (activeClientList.size() + retireClientList.size() >= clientPool.getMaxTotalPerKey()) {
                return null;
            }
            // TODO: how to check borrowed clients exceeds MaxTotal?
            // if the number of borrowed clients is less than MaxTotalPerKey but the total borrowed clients of all keys
            // exceeds MaxTotal, clientPool.borrowObject() will throw an exception "Timeout waiting for idle object".
            return clientPool.borrowObject(this.key);
        } catch (Exception e) {
            // the pool might return timeout exception if it could not get a client in PoolConfig.maxBlockWaitDuration
            // fetchFromPool() is internal use, return null here, let the caller handle.
            logger.error("Failed to get client, exception: ", e);
            return null;
        }
    }

    private void returnToPool(T grpcClient) {
        try {
            clientPool.returnObject(this.key, grpcClient);
        } catch (Exception e) {
            // the pool might return exception if the key doesn't exist or the grpcClient doesn't belong to this pool
            // returnToPool is internal use, the client must be in this pool, mute the exception
            logger.error("Failed to return client, exception: ", e);
        }
    }

    public float fetchClientPerSecond() {
        return this.fetchClientPerSecond;
    }

    private static class ClientWrapper<T> {
        private final T client;
        private final AtomicInteger refCount = new AtomicInteger(0);

        public ClientWrapper(T client) {
            this.client = client;
        }

        @Override
        public int hashCode() {
            // the hash code of ClientWrapper is equal to MilvusClient hash code
            return this.client.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            if (obj == null) {
                return false;
            }

            // obj is ClientWrapper
            if (this.getClass() == obj.getClass()) {
                return Objects.equals(this.client, ((ClientWrapper<?>) obj).client);
            }

            // obj is MilvusClient
            if (this.client != null && this.client.getClass() == obj.getClass()) {
                return Objects.equals(this.client, obj);
            }
            return false;
        }

        public T getClient() {
            this.refCount.incrementAndGet();
            return this.client;
        }

        public void returnClient() {
            this.refCount.decrementAndGet();
        }

        public int getRefCount() {
            return refCount.get();
        }
    }
}
