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

package io.milvus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.response.*;
import java.util.*;


// Note: read the following description before running this example
// 1. cmd into the "tls" folder, generate certificate by the following commands.
// (more details read the https://milvus.io/docs/tls.md)
//   chmod +x gen.sh
//   ./gen.sh
//
// 2. Configure the file paths of server.pem, server.key, and ca.pem for the server in config/milvus.yaml.
//    Set tlsMode to 1 for one-way authentication. Set tlsMode to 2 for two-way authentication.
// (read the doc to know how to config milvus: https://milvus.io/docs/configure-docker.md)
//    tls:
//        serverPemPath: [path_to_tls]/tls/server.pem
//        serverKeyPath: [path_to_tls]/tls/server.key
//        caPemPath: [path_to_tls]/tls/ca.pem
//
//    common:
//        security:
//            tlsMode: 2
//
// 3. Start milvus server
// 4. Run this example.
//    Connect server by oneWayAuth() if the server tlsMode=1, connect server by twoWayAuth() if the server tlsMode=2.
//
public class TLSExample {

    private static void oneWayAuth() {
        String path = ClassLoader.getSystemResource("").getPath();
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withServerName("localhost")
                .withServerPemPath(path + "/tls/server.pem")
                .build();
        MilvusServiceClient milvusClient = new MilvusServiceClient(connectParam);

        R<CheckHealthResponse> health = milvusClient.checkHealth();
        if (health.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(health.getMessage());
        } else {
            System.out.println(health);
        }
    }

    private static void twoWayAuth() {
        String path = ClassLoader.getSystemResource("").getPath();
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withServerName("localhost")
                .withCaPemPath(path + "/tls/ca.pem")
                .withClientKeyPath(path + "/tls/client.key")
                .withClientPemPath(path + "/tls/client.pem")
                .build();
        MilvusServiceClient milvusClient = new MilvusServiceClient(connectParam);

        R<CheckHealthResponse> health = milvusClient.checkHealth();
        if (health.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(health.getMessage());
        } else {
            System.out.println(health);
        }
    }

    // tlsMode=1, set oneWay=true
    // tlsMode=2, set oneWay=false
    private static final boolean oneWay = false;

    public static void main(String[] args) {
        if (oneWay) {
            oneWayAuth();
        } else {
            twoWayAuth();
        }
    }
}
