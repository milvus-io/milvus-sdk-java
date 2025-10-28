package io.milvus.pool;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ClientPool<C, T> {
    protected static final Logger logger = LoggerFactory.getLogger(ClientPool.class);
    protected GenericKeyedObjectPool<String, T> clientPool;
    protected PoolConfig config;
    protected PoolClientFactory<C, T> clientFactory;

    protected ClientPool() {

    }

    protected ClientPool(PoolConfig config, PoolClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;

        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
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

    public C removeConfig(String key) {
        return this.clientFactory.removeConfig(key);
    }

    public Set<String> configKeys() {
        return this.clientFactory.configKeys();
    }

    public C getConfig(String key) {
        return this.clientFactory.getConfig(key);
    }

    /**
     * Get a client object which is idle from the pool.
     * Once the client is hold by the caller, it will be marked as active state and cannot be fetched by other caller.
     * If the number of clients hits the MaxTotalPerKey value, this method will be blocked for MaxBlockWaitDuration.
     * If no idle client available after MaxBlockWaitDuration, this method will return a null object to caller.
     *
     * @param key the key of a group where the client belong
     * @return MilvusClient or MilvusClientV2
     */
    public T getClient(String key) {
        try {
            return clientPool.borrowObject(key);
        } catch (Exception e) {
            // the pool might return timeout exception if it could not get a client in PoolConfig.maxBlockWaitDuration
            logger.error("Failed to get client, exception: ", e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
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
        try {
            clientPool.returnObject(key, grpcClient);
        } catch (Exception e) {
            // the pool might return exception if the key doesn't exist or the grpcClient doesn't belong to this pool
            logger.error("Failed to return client, exception: " + e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
    }

    /**
     * Release/disconnect all clients of all key groups, close the pool.
     *
     */
    public void close() {
        if (clientPool != null && !clientPool.isClosed()) {
            clientPool.close();
            clientPool = null;
        }
    }

    /**
     * Release/disconnect idle clients of all key groups.
     *
     */
    public void clear() {
        if (clientPool != null && !clientPool.isClosed()) {
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
            clientPool.clear(key);
        }
    }

    /**
     * Return the number of idle clients of a key group
     *
     * @param key the key of a group
     */
    public int getIdleClientNumber(String key) {
        return clientPool.getNumIdle(key);
    }

    /**
     * Return the number of active clients of a key group
     *
     * @param key the key of a group
     */
    public int getActiveClientNumber(String key) {
        return clientPool.getNumActive(key);
    }

    /**
     * Return the number of idle clients of all key group
     *
     */
    public int getTotalIdleClientNumber() {
        return clientPool.getNumIdle();
    }

    /**
     * Return the number of active clients of all key group
     *
     */
    public int getTotalActiveClientNumber() {
        return clientPool.getNumActive();
    }
}
