package io.milvus.pool;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ClientPool<C, T> {
    protected static final Logger logger = LoggerFactory.getLogger(ClientPool.class);
    protected GenericKeyedObjectPool<String, T> clientPool;
    protected PoolConfig config;
    protected PoolClientFactory<C, T> clientFactory;
    private final ConcurrentMap<String, ClientCache<T>> clientsCache = new ConcurrentHashMap<>();
    private final Lock cacheMapLock = new ReentrantLock(true);

    protected ClientPool() {

    }

    protected ClientPool(PoolConfig config, PoolClientFactory<C, T> clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;

        GenericKeyedObjectPoolConfig<T> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setMaxIdlePerKey(config.getMaxIdlePerKey());
        poolConfig.setMinIdlePerKey(config.getMinIdlePerKey());
        poolConfig.setMaxTotal(config.getMaxTotal());
        poolConfig.setMaxTotalPerKey(config.getMaxTotalPerKey());
        poolConfig.setBlockWhenExhausted(config.isBlockWhenExhausted());
        poolConfig.setMaxWait(config.getMaxBlockWaitDuration());
        poolConfig.setTestOnBorrow(config.isTestOnBorrow());
        poolConfig.setTestOnReturn(config.isTestOnReturn());
        poolConfig.setTestOnCreate(false);
        poolConfig.setTestWhileIdle(false);
        poolConfig.setTimeBetweenEvictionRuns(config.getEvictionPollingInterval());
        poolConfig.setNumTestsPerEvictionRun(5);
        poolConfig.setMinEvictableIdleTime(config.getMinEvictableIdleDuration());
        this.clientPool = new GenericKeyedObjectPool<String, T>(clientFactory, poolConfig);
    }

    public void configForKey(String key, C config) {
        this.clientFactory.configForKey(key, config);
    }

    public void removeConfig(String key) {
        this.clientFactory.removeConfig(key);
    }

    public Set<String> configKeys() {
        return this.clientFactory.configKeys();
    }

    public C getConfig(String key) {
        return this.clientFactory.getConfig(key);
    }

    /**
     * Create minIdlePerKey clients for the pool of the key.
     * Call this method before business can reduce the latency of the first time to getClient().
     */
    public void preparePool(String key) {
        ClientCache<T> cache = getCache(key);
        if (cache != null) {
            cache.preparePool();
        }
    }

    /**
     * Get a client object from the cache. If the cache is empty, it will fetch a client from the underlying pool.
     * The cache maintains a list of clients. The cache will increase the ref-count of this client when it is fetched
     * by getClient(), and decrease the ref-count when it is returned by returnClient().
     * The cache balances the caller to multiple clients according to the ref-count of each client. getClient() will
     * return the client which has the minimum ref-count to the caller.
     * If the average ref-count of clients is smaller than a threshold, the cache will retire a client which has
     * the maximum ref-count, wait its ref-count to be zero and return it to the underlying pool.
     * If the number of clients hits the MaxTotalPerKey value, this method will be blocked for MaxBlockWaitDuration.
     * If no idle client available after MaxBlockWaitDuration, this method will return a null object to caller.
     *
     * @param key the key of a group where the client belong
     * @return MilvusClient or MilvusClientV2
     */
    public T getClient(String key) {
        ClientCache<T> cache = getCache(key);
        if (cache == null) {
            logger.error("Not able to create a client cache for key: {}", key);
            return null;
        }
        return cache.getClient();
    }

    private ClientCache<T> getCache(String key) {
        ClientCache<T> cache = clientsCache.get(key);
        if (cache == null) {
            // If clientsCache doesn't contain this key, there might be multiple threads run into this section.
            // Although ConcurrentMap.putIfAbsent() is atomic action, we don't intend to allow multiple threads
            // to create multiple ClientCache objects at this line, so we add a lock here.
            // Only one thread that first obtains the lock runs into putIfAbsent(), the others will be blocked
            // and get the object after obtaining the lock.
            // This section is entered one time for each key, the lock basically doesn't affect performance.
            cacheMapLock.lock();
            try {
                if (!clientsCache.containsKey(key)) {
                    cache = new ClientCache<>(key, clientPool);
                    clientsCache.put(key, cache);
                } else {
                    cache = clientsCache.get(key);
                }
            } finally {
                cacheMapLock.unlock();
            }
        }
        return cache;
    }

    /**
     * Return a client object. Once a client is returned, it becomes idle state and wait the next caller.
     * The caller should ensure the client is returned. Otherwise, the client will keep in active state and cannot be used by the next caller.
     * Throw exceptions if the key doesn't exist or the client is not belong to this key group.
     *
     * @param key        the key of a group where the client belong
     * @param grpcClient the client object to return
     */
    public void returnClient(String key, T grpcClient) {
        ClientCache<T> cache = clientsCache.get(key);
        if (cache != null) {
            cache.returnClient(grpcClient);
        } else {
            logger.warn("No such key: {}", key);
        }
    }

    /**
     * Release/disconnect all clients of all key groups, close the pool.
     *
     */
    public void close() {
        if (clientPool != null && !clientPool.isClosed()) {
            // how about if clientPool and clientsCache are cleared but some clients are not returned?
            // after clear(), all the milvus clients will be closed, if user continue to use the unreturned client
            // to call api, the client will receive a io.grpc.Status.UNAVAILABLE error and retry the call
            clear();
            clientPool.close();
            clientPool = null;
        }
    }

    /**
     * Release/disconnect idle clients of all key groups.
     */
    public void clear() {
        if (clientPool != null && !clientPool.isClosed()) {
            // how about if clientPool and clientsCache are cleared but some clients are not returned?
            // after clear(), all the milvus clients will be closed, if user continue to use the unreturned client
            // to call api, the client will receive a io.grpc.Status.UNAVAILABLE error and retry the call
            for (ClientCache<T> cache : clientsCache.values()) {
                cache.stopTimer();
            }
            clientsCache.clear();
            clientPool.clear();
        }
    }

    /**
     * Release/disconnect idle clients of a key group.
     *
     * @param key the key of a group
     */
    public void clear(String key) {
        if (clientPool != null && !clientPool.isClosed()) {
            // how about if clientPool and clientsCache are cleared but some clients are not returned?
            // after clear(), all the milvus clients will be closed, if user continue to use the unreturned client
            // to call api, the client will receive a io.grpc.Status.UNAVAILABLE error and retry the call
            ClientCache<T> cache = clientsCache.get(key);
            if (cache != null) {
                cache.stopTimer();
            }
            clientsCache.remove(key);
            clientPool.clear(key);
        }
    }

    /**
     * Return the number of idle clients of a key group
     * Threadsafe method.
     *
     * @param key the key of a group
     */
    public int getIdleClientNumber(String key) {
        return clientPool.getNumIdle(key);
    }

    /**
     * Return the number of active clients of a key group
     * Threadsafe method.
     *
     * @param key the key of a group
     */
    public int getActiveClientNumber(String key) {
        return clientPool.getNumActive(key);
    }

    /**
     * Return the number of idle clients of all key group
     * Threadsafe method.
     */
    public int getTotalIdleClientNumber() {
        return clientPool.getNumIdle();
    }

    /**
     * Return the number of active clients of all key group
     * Threadsafe method.
     */
    public int getTotalActiveClientNumber() {
        return clientPool.getNumActive();
    }

    public float fetchClientPerSecond(String key) {
        ClientCache<T> cache = clientsCache.get(key);
        if (cache != null) {
            return cache.fetchClientPerSecond();
        }
        return 0.0F;
    }
}
