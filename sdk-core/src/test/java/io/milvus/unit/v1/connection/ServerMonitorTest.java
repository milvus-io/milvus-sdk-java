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

package io.milvus.unit.v1.connection;

import io.milvus.client.MilvusClient;
import io.milvus.connection.ClusterFactory;
import io.milvus.connection.ServerMonitor;
import io.milvus.connection.ServerSetting;
import io.milvus.param.QueryNodeSingleSearch;
import io.milvus.param.ServerAddress;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class ServerMonitorTest {

    private ServerSetting createServerSetting() {
        return ServerSetting.newBuilder()
                .withHost(ServerAddress.newBuilder().build())
                .withMilvusClient(mock(MilvusClient.class))
                .build();
    }

    private QueryNodeSingleSearch createSingleSearch() {
        return QueryNodeSingleSearch.newBuilder()
                .withCollectionName("heartbeat_coll")
                .withVectorFieldName("float_vector")
                .withVectors(Collections.singletonList(Arrays.asList(1.0f, 2.0f)))
                .build();
    }

    @Test
    void testConstructorWithQueryNodeSearch() {
        ClusterFactory factory = mock(ClusterFactory.class);
        ServerMonitor monitor = new ServerMonitor(factory, createSingleSearch());
        assertNotNull(monitor);
    }

    @Test
    void testHeartbeatLoopElectsMasterWhenNotRunning() {
        ClusterFactory factory = mock(ClusterFactory.class);
        ServerSetting setting = createServerSetting();
        when(factory.getServerSettings()).thenReturn(Collections.emptyList());
        when(factory.masterIsRunning()).thenReturn(false);
        when(factory.electMaster()).thenReturn(setting);

        ServerMonitor monitor = new ServerMonitor(factory, null);
        monitor.start();
        try {
            verify(factory, timeout(3000)).electMaster();
            verify(factory, timeout(3000)).masterChange(setting);
            verify(factory, timeout(3000)).availableServerChange(Collections.emptyList());
        } finally {
            monitor.close();
        }
    }

    @Test
    void testHeartbeatLoopSkipsElectionWhenMasterRunning() {
        ClusterFactory factory = mock(ClusterFactory.class);
        when(factory.getServerSettings()).thenReturn(Collections.emptyList());
        when(factory.masterIsRunning()).thenReturn(true);

        ServerMonitor monitor = new ServerMonitor(factory, null);
        monitor.start();
        try {
            verify(factory, timeout(3000)).availableServerChange(Collections.emptyList());
            verify(factory, never()).electMaster();
        } finally {
            monitor.close();
        }
    }
}
