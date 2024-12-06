package io.milvus.pool;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

public class MilvusClientV2Pool extends ClientPool<ConnectConfig, MilvusClientV2> {
    public MilvusClientV2Pool(PoolConfig poolConfig, ConnectConfig connectConfig) throws ClassNotFoundException, NoSuchMethodException {
        super(poolConfig, new PoolClientFactory<ConnectConfig, MilvusClientV2>(connectConfig, MilvusClientV2.class.getName()));
    }
}
