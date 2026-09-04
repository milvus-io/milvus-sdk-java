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

package io.milvus.integration.v2.client;

import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ConnectConfigTest extends BaseTest {
    @Test
    void connectConfigDefaults() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://dummyHost:19530")
                .build();
        Assertions.assertEquals(10000, config.getKeepAliveTimeMs());
        Assertions.assertEquals(5000, config.getKeepAliveTimeoutMs());
        Assertions.assertTrue(config.isKeepAliveWithoutCalls());
    }

    @Test
    void connectConfigToStringRedactsCredentials() {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://uri-user:uri-password@dummyHost:19530/default")
                .token("sensitive-token")
                .username("sensitive-user")
                .password("sensitive-password")
                .dbName("default")
                .build();

        String loggedConfig = config.toString();
        Assertions.assertFalse(loggedConfig.contains("sensitive-token"));
        Assertions.assertFalse(loggedConfig.contains("sensitive-password"));
        Assertions.assertFalse(loggedConfig.contains("uri-user"));
        Assertions.assertFalse(loggedConfig.contains("uri-password"));
        Assertions.assertTrue(loggedConfig.contains("token='<redacted>'"));
        Assertions.assertTrue(loggedConfig.contains("username='sensitive-user'"));
        Assertions.assertTrue(loggedConfig.contains("password='<redacted>'"));
        Assertions.assertTrue(loggedConfig.contains(
                "uri='http://<redacted>@dummyHost:19530/default'"));
        Assertions.assertTrue(loggedConfig.contains("dbName='default'"));
    }
}
