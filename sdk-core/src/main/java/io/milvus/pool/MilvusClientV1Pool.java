package io.milvus.pool;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;

public class MilvusClientV1Pool extends ClientPool<ConnectParam, MilvusClient> {
    public MilvusClientV1Pool(PoolConfig poolConfig, ConnectParam connectParam) throws ClassNotFoundException, NoSuchMethodException {
        super(poolConfig, new PoolClientFactory<ConnectParam, MilvusClient>(connectParam, MilvusServiceClient.class.getName()));
    }
}
