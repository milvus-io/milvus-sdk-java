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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keyed factory that creates, wraps, validates and destroys Milvus client objects for an Apache
 * Commons Pool keyed by endpoint.
 *
 * <p>The factory discovers the client constructor and lifecycle methods ({@code close} and
 * {@code clientIsReady}) reflectively from the client class name. A default config is used for keys
 * without a dedicated config.
 *
 * @param <C> the client config type, such as {@code ConnectParam} or {@code ConnectConfig}
 * @param <T> the client type, such as {@code MilvusClient} or {@code MilvusClientV2}
 */
public class PoolClientFactory<C, T> extends BaseKeyedPooledObjectFactory<String, T> {
    protected static final Logger logger = LoggerFactory.getLogger(PoolClientFactory.class);
    private final C configDefault;
    private ConcurrentMap<String, C> configForKeys = new ConcurrentHashMap<>();
    private Constructor<?> constructor;
    private Method closeMethod;
    private Method verifyMethod;

    /**
     * Creates a pool client factory for the given default config and client class name.
     *
     * @param configDefault   the default config used for keys without a dedicated config
     * @param clientClassName the fully qualified name of the client class
     * @throws ClassNotFoundException if the client class or its config class cannot be found
     * @throws NoSuchMethodException  if the client class lacks the constructor or lifecycle methods
     */
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

    /**
     * Associates a dedicated config with the given pool key.
     *
     * @param key    the pool key, typically the endpoint
     * @param config the config to use for clients created for this key
     */
    public void configForKey(String key, C config) {
        configForKeys.put(key, config);
    }

    /**
     * Removes the dedicated config associated with the given pool key.
     *
     * @param key the pool key
     * @return the removed config, or {@code null} if no config was associated with the key
     */
    public C removeConfig(String key) {
        return configForKeys.remove(key);
    }

    /**
     * Returns the set of pool keys that have a dedicated config.
     *
     * @return the set of config keys
     */
    public Set<String> configKeys() {
        return configForKeys.keySet();
    }

    /**
     * Returns the config associated with the given pool key.
     *
     * @param key the pool key
     * @return the config, or {@code null} if no config is associated with the key
     */
    public C getConfig(String key) {
        return configForKeys.get(key);
    }

    /**
     * Creates a new client object for the given pool key, using the key-specific config when
     * present and the default config otherwise.
     *
     * @param key the pool key
     * @return the created client
     */
    @Override
    public T create(String key) throws Exception {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("PoolClientFactory key: {} creates a client", key);
            }
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

    /**
     * Wraps a created client into a pooled object.
     *
     * @param client the client to wrap
     * @return the pooled object wrapping the client
     */
    @Override
    public PooledObject<T> wrap(T client) {
        return new DefaultPooledObject<>(client);
    }

    /**
     * Closes a client before it is destroyed by the pool, invoking its {@code close} method.
     *
     * @param key the pool key
     * @param p   the pooled object holding the client
     */
    @Override
    public void destroyObject(String key, PooledObject<T> p) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("PoolClientFactory key: {} closes a client", key);
        }
        T client = p.getObject();
        closeMethod.invoke(client, 3L);
    }

    /**
     * Validates that a pooled client is still usable by invoking its {@code clientIsReady} method.
     *
     * @param key the pool key
     * @param p   the pooled object holding the client
     * @return {@code true} if the client is ready for use
     */
    @Override
    public boolean validateObject(String key, PooledObject<T> p) {
        try {
            T client = p.getObject();
            return (boolean) verifyMethod.invoke(client);
        } catch (Exception e) {
            logger.error("Failed to validate client, exception: ", e);
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e);
        }
    }

    /**
     * Activates a borrowed client before it is handed to a caller.
     *
     * @param key the pool key
     * @param p   the pooled object holding the client
     */
    @Override
    public void activateObject(String key, PooledObject<T> p) throws Exception {
        super.activateObject(key, p);
    }

    /**
     * Passivates a returned client before it is stored back in the pool.
     *
     * @param key the pool key
     * @param p   the pooled object holding the client
     */
    @Override
    public void passivateObject(String key, PooledObject<T> p) throws Exception {
        super.passivateObject(key, p);
    }
}
