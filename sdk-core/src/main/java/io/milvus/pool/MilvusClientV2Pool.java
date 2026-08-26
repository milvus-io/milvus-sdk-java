package io.milvus.pool;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

/**
 * Pool of v2 {@link MilvusClientV2} instances created from a {@link ConnectConfig} config.
 *
 * <p>Clients are created via the {@code MilvusClientV2} constructor and managed by the underlying
 * {@link ClientPool}. Use {@code getClient}/{@code returnClient} to borrow and return clients keyed
 * by endpoint.
 */
public class MilvusClientV2Pool extends ClientPool<ConnectConfig, MilvusClientV2> {
    /**
     * Creates a pool of v2 Milvus clients.
     *
     * @param poolConfig   the pool configuration
     * @param connectConfig the default connect configuration used to create clients
     * @throws ClassNotFoundException if the client class cannot be found
     * @throws NoSuchMethodException  if the client class lacks the expected constructor or methods
     */
    public MilvusClientV2Pool(PoolConfig poolConfig, ConnectConfig connectConfig) throws ClassNotFoundException, NoSuchMethodException {
        super(poolConfig, new PoolClientFactory<ConnectConfig, MilvusClientV2>(connectConfig, MilvusClientV2.class.getName()));
    }
}
