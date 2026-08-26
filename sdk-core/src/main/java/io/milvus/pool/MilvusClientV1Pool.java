package io.milvus.pool;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;

/**
 * Pool of v1 {@link MilvusClient} instances created from a {@link ConnectParam} config.
 *
 * <p>Clients are created reflectively via {@link MilvusServiceClient} and managed by the underlying
 * {@link ClientPool}. Use {@code getClient}/{@code returnClient} to borrow and return clients keyed
 * by endpoint.
 */
public class MilvusClientV1Pool extends ClientPool<ConnectParam, MilvusClient> {
    /**
     * Creates a pool of v1 Milvus clients.
     *
     * @param poolConfig   the pool configuration
     * @param connectParam the default connect parameters used to create clients
     * @throws ClassNotFoundException if the client class cannot be found
     * @throws NoSuchMethodException  if the client class lacks the expected constructor or methods
     */
    public MilvusClientV1Pool(PoolConfig poolConfig, ConnectParam connectParam) throws ClassNotFoundException, NoSuchMethodException {
        super(poolConfig, new PoolClientFactory<ConnectParam, MilvusClient>(connectParam, MilvusServiceClient.class.getName()));
    }
}
