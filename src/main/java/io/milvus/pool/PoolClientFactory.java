package io.milvus.pool;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class PoolClientFactory<C, T> extends BaseKeyedPooledObjectFactory<String, T> {
    private final C config;
    private Constructor<?> constructor;
    private Method closeMethod;
    private Method verifyMethod;

    public PoolClientFactory(C config, String clientClassName) throws ClassNotFoundException, NoSuchMethodException {
        this.config = config;
        try {
            Class<?> clientCls = Class.forName(clientClassName);
            Class<?> configCls = Class.forName(config.getClass().getName());
            constructor = clientCls.getConstructor(configCls);
            closeMethod = clientCls.getMethod("close", long.class);
            verifyMethod = clientCls.getMethod("clientIsReady");
        } catch (Exception e) {
            System.out.println("Failed to create client pool factory, exception: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public T create(String key) throws Exception {
        try {
            T client = (T) constructor.newInstance(this.config);
            return client;
        } catch (Exception e) {
            return null;
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
            System.out.println("Failed to validate client, exception: " + e.getMessage());
            return true;
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
