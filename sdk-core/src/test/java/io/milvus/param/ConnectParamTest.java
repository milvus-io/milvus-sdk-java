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

package io.milvus.param;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectParamTest {
    private static final String SERVERLESS_URI =
            "https://in05-89094193939e0d9.serverless.gcp-us-west1.cloud.zilliz.com";

    @Test
    void serverlessUriUsesEmptyDatabaseName() throws Exception {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri(SERVERLESS_URI)
                .build();

        assertEquals("", connectParam.getDatabaseName());
        assertEquals(443, connectParam.getPort());
    }

    @Test
    void explicitDatabaseIsPreservedForServerlessUri() throws Exception {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri(SERVERLESS_URI)
                .withDatabaseName("request_db")
                .build();

        assertEquals("request_db", connectParam.getDatabaseName());
    }

    @Test
    void regularUriUsesEmptyDatabaseWhenPathIsEmpty() throws Exception {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri("http://localhost:19530")
                .build();

        assertEquals("", connectParam.getDatabaseName());
    }

    @Test
    void hostAndPortConnectionUsesEmptyDatabaseByDefault() throws Exception {
        ConnectParam connectParam = ConnectParam.newBuilder().build();

        assertEquals("", connectParam.getDatabaseName());
    }

    @Test
    void regularUriUsesDatabaseFromPath() throws Exception {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri("http://localhost:19530/request_db")
                .build();

        assertEquals("request_db", connectParam.getDatabaseName());
    }
}
