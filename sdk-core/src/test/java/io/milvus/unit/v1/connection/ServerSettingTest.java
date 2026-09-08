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
import io.milvus.connection.ServerSetting;
import io.milvus.exception.ParamException;
import io.milvus.param.ServerAddress;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("unit")
class ServerSettingTest {

    @Test
    void testBuildAndGetters() {
        ServerAddress address = ServerAddress.newBuilder().withHost("localhost").withPort(19530).build();
        MilvusClient client = mock(MilvusClient.class);
        ServerSetting setting = ServerSetting.newBuilder()
                .withHost(address)
                .withMilvusClient(client)
                .build();
        assertSame(address, setting.getServerAddress());
        assertSame(client, setting.getClient());
    }

    @Test
    void testPublicConstructorBypassesValidation() {
        ServerAddress address = ServerAddress.newBuilder().build();
        MilvusClient client = mock(MilvusClient.class);
        ServerSetting setting = new ServerSetting(ServerSetting.newBuilder()
                .withHost(address)
                .withMilvusClient(client));
        assertEquals(address, setting.getServerAddress());
        assertSame(client, setting.getClient());
    }

    @Test
    void testConstructorRejectsNullBuilder() {
        assertThrows(IllegalArgumentException.class, () -> new ServerSetting(null));
    }

    @Test
    void testWithHostRejectsNullAddress() {
        assertThrows(IllegalArgumentException.class,
                () -> ServerSetting.newBuilder().withHost(null));
    }

    @Test
    void testBuildRejectsNullClient() {
        ServerAddress address = ServerAddress.newBuilder().build();
        assertThrows(ParamException.class,
                () -> ServerSetting.newBuilder().withHost(address).build());
    }

    @Test
    void testBuildRejectsOutOfRangePort() {
        ServerAddress address = mock(ServerAddress.class);
        when(address.getHost()).thenReturn("localhost");
        when(address.getPort()).thenReturn(70000);
        MilvusClient client = mock(MilvusClient.class);
        assertThrows(ParamException.class,
                () -> ServerSetting.newBuilder().withHost(address).withMilvusClient(client).build());
    }

    @Test
    void testBuildRejectsEmptyHost() {
        ServerAddress address = mock(ServerAddress.class);
        when(address.getHost()).thenReturn("");
        MilvusClient client = mock(MilvusClient.class);
        assertThrows(ParamException.class,
                () -> ServerSetting.newBuilder().withHost(address).withMilvusClient(client).build());
    }
}
