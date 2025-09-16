package io.milvus.pool;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PoolClientFactory<C, T> extends BaseKeyedPooledObjectFactory<String, T> {
    protected static final Logger logger = LoggerFactory.getLogger(PoolClientFactory.class);
    private final C configDefault;
    private ConcurrentMap<String, C> configForKeys = new ConcurrentHashMap<>();
    private Constructor<?> constructor;
    private Method closeMethod;
    private Method verifyMethod;

    public PoolClientFactory(C configDefault, String clientClassName) throws ClassNotFoundException, NoSuchMethodException {
        this.configDefault = configDefault;
        try {
            Class<?> clientCls = Class.forName(clientClassName);
            Class<?> configCls = Class.forName(configDefault.getClass().getName());
            constructor = clientCls.getConstructor(configCls);
            closeMethod = clientCls.getMethod("close", long.class);
            verifyMethod = clientCls.getMethod("clientIsReady");
        } catch (Exception e) {
            logger.error("Failed to create client pool factory, exception: ", e);
            throw e;
        }
    }

    public void configForKey(String key, C config) {
        configForKeys.put(key, config);
    }

    @Override
    public T create(String key) throws Exception {
        try {
            C keyConfig = configForKeys.get(key);
            if (keyConfig == null) {
                return (T) constructor.newInstance(this.configDefault);
            } else {
                return (T) constructor.newInstance(keyConfig);
            }
        } catch (Exception e) {
            logger.error("Failed to create client, exception: ", e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
    }

    @Override
    public PooledObject<T> wrap(T client) {
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(String key, PooledObject<T> p) throws Exception {
        T client = p.getObject();
        closeMethod.invoke(client, 3L);
    }

    @Override
    public boolean validateObject(String key, PooledObject<T> p) {
        try {
            T client = p.getObject();
            return (boolean) verifyMethod.invoke(client);
        } catch (Exception e) {
            logger.error("Failed to validate client, exception: " + e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
    }

    @Override
    public void activateObject(String key, PooledObject<T> p) throws Exception {
        super.activateObject(key, p);
    }

    @Override
    public void passivateObject(String key, PooledObject<T> p) throws Exception {
        super.passivateObject(key, p);
    }
}
