package io.milvus.pool;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.time.Duration;

public class ClientPool<C, T> {
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

    public T getClient(String key) {
        try {
            return clientPool.borrowObject(key);
        } catch (Exception e) {
            System.out.println("Failed to get client, exception: " + e.getMessage());
            return null;
        }
    }


    public void returnClient(String key, T grpcClient) {
        try {
            clientPool.returnObject(key, grpcClient);
        } catch (Exception e) {
            System.out.println("Failed to return client, exception: " + e.getMessage());
            throw e;
        }
    }

    public void close() {
        if (clientPool != null && !clientPool.isClosed()) {
            clientPool.close();
            clientPool = null;
        }
    }

    public void clear() {
        if (clientPool != null && !clientPool.isClosed()) {
            clientPool.clear();
        }
    }

    public void clear(String key) {
        if (clientPool != null && !clientPool.isClosed()) {
            clientPool.clear(key);
        }
    }

    public int getIdleClientNumber(String key) {
        return clientPool.getNumIdle(key);
    }

    public int getActiveClientNumber(String key) {
        return clientPool.getNumActive(key);
    }

    public int getTotalIdleClientNumber() {
        return clientPool.getNumIdle();
    }

    public int getTotalActiveClientNumber() {
        return clientPool.getNumActive();
    }
}
