package io.milvus.v2;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

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
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .serverName("localhost")
                .serverPemPath(path + "/server.pem")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        String version = client.getServerVersion();
        System.out.println("Server version: " + version);
    }

    private static void twoWayAuth() {
        ClassLoader classLoader = TLSExample.class.getClassLoader();
        URL resourceUrl = classLoader.getResource("tls");
        String path = new File(resourceUrl.getFile()).getAbsolutePath();
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .serverName("localhost")
                .caPemPath(path + "/ca.pem")
                .clientKeyPath(path + "/client.key")
                .clientPemPath(path + "/client.pem")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        String version = client.getServerVersion();
        System.out.println("Server version: " + version);
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
