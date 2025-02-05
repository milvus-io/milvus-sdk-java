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
package io.milvus.v1;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetVersionResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;

import java.io.File;
import java.net.URL;


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
        ClassLoader classLoader = TLSExample.class.getClassLoader();
        URL resourceUrl = classLoader.getResource("tls");
        String path = new File(resourceUrl.getFile()).getAbsolutePath();
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withServerName("localhost")
                .withServerPemPath(path + "/server.pem")
                .build();
        MilvusServiceClient client = new MilvusServiceClient(connectParam);

        R<GetVersionResponse> version = client.getVersion();
        if (version.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(version.getMessage());
        } else {
            System.out.println("Server version: " + version.getData().getVersion());
        }
    }

    private static void twoWayAuth() {
        ClassLoader classLoader = TLSExample.class.getClassLoader();
        URL resourceUrl = classLoader.getResource("tls");
        String path = new File(resourceUrl.getFile()).getAbsolutePath();
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withServerName("localhost")
                .withCaPemPath(path + "/ca.pem")
                .withClientKeyPath(path + "/client.key")
                .withClientPemPath(path + "/client.pem")
                .build();
        MilvusServiceClient client = new MilvusServiceClient(connectParam);

        R<GetVersionResponse> version = client.getVersion();
        if (version.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(version.getMessage());
        } else {
            System.out.println("Server version: " + version.getData().getVersion());
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
