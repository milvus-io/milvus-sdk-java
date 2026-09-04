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

package io.milvus.system.v1.client;

import io.milvus.support.TestUtils;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.client.MilvusClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tag("system")
class ClientPoolDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testClientPool() {
        try {
            ConnectParam connectParam = ConnectParam.newBuilder()
                    .withUri(TestUtils.MilvusStandaloneUri)
                    .build();
            int minIdlePerKey = 1;
            int maxIdlePerKey = 2;
            int maxTotalPerKey = 4;
            PoolConfig poolConfig = PoolConfig.builder()
                    .minIdlePerKey(minIdlePerKey)
                    .maxIdlePerKey(maxIdlePerKey)
                    .maxTotalPerKey(maxTotalPerKey)
                    .build();
            MilvusClientV1Pool pool = new MilvusClientV1Pool(poolConfig, connectParam);

            String key = "dummy";
            pool.preparePool(key);
            Assertions.assertEquals(minIdlePerKey, pool.getActiveClientNumber(key));

            List<Thread> threadList = new ArrayList<>();
            int threadCount = 20;
            int requestPerThread = 10000;
            for (int k = 0; k < threadCount; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < requestPerThread; i++) {
                        MilvusClient client = null;
                        try {
                            client = pool.getClient(key);
                            R<GetVersionResponse> resp = client.getVersion();
                            Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus().intValue());
//                        System.out.printf("%d, %s%n", i, resp.getData().getVersion());
//                        System.out.printf("idle %d, active %d%n", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
                        } catch (Exception e) {
                            System.out.printf("request failed: %s%n", e);
                        } finally {
                            pool.returnClient(key, client);
                        }
                    }
                    System.out.printf("Thread %s finished%n", Thread.currentThread().getName());
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }
            Assertions.assertEquals(maxTotalPerKey, pool.getActiveClientNumber(key));
            Assertions.assertEquals(maxTotalPerKey, pool.getTotalActiveClientNumber());
            System.out.printf("qps: %.2f%n", pool.fetchClientPerSecond(key));

            while (pool.getActiveClientNumber(key) > 1) {
                TimeUnit.SECONDS.sleep(1);
                System.out.printf("waiting idle %d, active %d%n", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
            }
            Assertions.assertEquals(maxIdlePerKey, pool.getIdleClientNumber(key));
            Assertions.assertEquals(maxIdlePerKey, pool.getTotalIdleClientNumber());
            Assertions.assertEquals(1, pool.getActiveClientNumber(key));
            Assertions.assertEquals(1, pool.getTotalActiveClientNumber());
            pool.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }
}
