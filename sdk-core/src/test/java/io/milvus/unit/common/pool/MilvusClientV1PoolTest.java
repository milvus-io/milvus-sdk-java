/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.unit.common.pool;

import io.milvus.param.ConnectParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.exception.MilvusClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class MilvusClientV1PoolTest {

    private static ConnectParam connectParam() throws Exception {
        return ConnectParam.newBuilder().withHost("localhost").withPort(19530).build();
    }

    @Test
    void constructsWithConnectParam() throws Exception {
        MilvusClientV1Pool pool = new MilvusClientV1Pool(PoolConfig.builder().build(), connectParam());
        Assertions.assertNotNull(pool);
        pool.close();
    }

    @Test
    void configDelegationWorks() throws Exception {
        ConnectParam param = connectParam();
        MilvusClientV1Pool pool = new MilvusClientV1Pool(PoolConfig.builder().build(), param);
        pool.configForKey("k", param);
        Assertions.assertSame(param, pool.getConfig("k"));
        Assertions.assertTrue(pool.configKeys().contains("k"));

        pool.removeConfig("k");
        Assertions.assertNull(pool.getConfig("k"));
        pool.close();
    }

    @Test
    void countsStartAtZero() throws Exception {
        MilvusClientV1Pool pool = new MilvusClientV1Pool(PoolConfig.builder().build(), connectParam());
        Assertions.assertEquals(0, pool.getTotalActiveClientNumber());
        Assertions.assertEquals(0, pool.getTotalIdleClientNumber());
        pool.close();
    }

    @Test
    void getClientAfterCloseThrows() throws Exception {
        MilvusClientV1Pool pool = new MilvusClientV1Pool(PoolConfig.builder().build(), connectParam());
        pool.close();
        Assertions.assertThrows(MilvusClientException.class, () -> pool.getClient("k"));
    }
}
