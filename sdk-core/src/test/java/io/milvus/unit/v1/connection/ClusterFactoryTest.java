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
import io.milvus.connection.ServerSetting;
import io.milvus.exception.ParamException;
import io.milvus.param.ServerAddress;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("unit")
class ClusterFactoryTest {

    private ServerSetting createServerSetting(String host) {
        ServerAddress address = ServerAddress.newBuilder().withHost(host).build();
        return ServerSetting.newBuilder()
                .withHost(address)
                .withMilvusClient(mock(MilvusClient.class))
                .build();
    }

    @Test
    void testNewBuilder() {
        assertNotNull(ClusterFactory.newBuilder());
    }

    @Test
    void testWithServerSettingRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> ClusterFactory.newBuilder().withServerSetting(null));
    }

    @Test
    void testBuildWithoutSettingsThrows() {
        assertThrows(ParamException.class, () -> ClusterFactory.newBuilder().build());
    }

    @Test
    void testBuildWithEmptySettingsThrows() {
        assertThrows(ParamException.class,
                () -> ClusterFactory.newBuilder().withServerSetting(Collections.emptyList()).build());
    }

    @Test
    void testBuildAndClusterState() {
        ServerSetting s1 = createServerSetting("s1");
        ServerSetting s2 = createServerSetting("s2");
        ClusterFactory factory = ClusterFactory.newBuilder()
                .withServerSetting(Arrays.asList(s1, s2))
                .keepMonitor(false)
                .build();

        assertEquals(2, factory.getServerSettings().size());
        assertSame(s1, factory.getDefaultServer());
        assertSame(s1, factory.getMaster());
        assertEquals(2, factory.getAvailableServerSettings().size());
        assertTrue(factory.masterIsRunning());
        assertSame(s1, factory.electMaster());
    }

    @Test
    void testMasterChange() {
        ServerSetting s1 = createServerSetting("s1");
        ServerSetting s2 = createServerSetting("s2");
        ClusterFactory factory = ClusterFactory.newBuilder()
                .withServerSetting(Arrays.asList(s1, s2))
                .build();

        factory.availableServerChange(Collections.singletonList(s2));
        assertEquals(1, factory.getAvailableServerSettings().size());
        assertSame(s2, factory.electMaster());
        assertFalse(factory.masterIsRunning());

        factory.masterChange(s2);
        assertSame(s2, factory.getMaster());
        assertTrue(factory.masterIsRunning());
    }

    @Test
    void testElectMasterFallsBackToDefaultWhenNoAvailable() {
        ServerSetting s1 = createServerSetting("s1");
        ServerSetting s2 = createServerSetting("s2");
        ClusterFactory factory = ClusterFactory.newBuilder()
                .withServerSetting(Arrays.asList(s1, s2))
                .build();

        factory.availableServerChange(Collections.emptyList());
        assertSame(s1, factory.electMaster());
    }

    @Test
    void testCloseWithoutMonitorIsNoOp() {
        ServerSetting s1 = createServerSetting("s1");
        ClusterFactory factory = ClusterFactory.newBuilder()
                .withServerSetting(Collections.singletonList(s1))
                .keepMonitor(false)
                .build();
        factory.close();
        List<ServerSetting> settings = factory.getServerSettings();
        assertEquals(1, settings.size());
    }
}
