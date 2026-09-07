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

package io.milvus.bulkwriter.storage.client;

import com.sun.net.httpserver.HttpServer;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.exception.ParamException;
import io.minio.BucketExistsArgs;
import io.minio.credentials.Credentials;
import io.minio.credentials.Provider;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class MinioStorageClientTest {

    private HttpServer server;
    private final AtomicReference<String> authorizationHeader = new AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            authorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            // no such bucket: drives bucketExists down the false path
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void buildsClientWithStaticKeys() throws Exception {
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .withRegion("us-west-2")
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        // static keys take the signing path
        assertSignedWithAccessKey("ak");
    }

    @Test
    void buildsClientWithExternalCredentialsProvider() throws Exception {
        Provider provider = () -> new Credentials("provider-ak", "provider-sk", "token", null);
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withRegion("us-west-2")
                .withCredentialsProvider(provider)
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        // the external provider, not a StaticProvider, signed the request
        assertSignedWithAccessKey("provider-ak");
    }

    @Test
    void credentialsProviderRejectsStaticKeys() {
        Provider provider = () -> new Credentials("ak", "sk", null, null);
        S3ConnectParam.Builder base = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withCredentialsProvider(provider);

        ParamException e = assertThrows(ParamException.class,
                () -> base.withAccessKey("ak").build());
        assertTrue(e.getMessage().contains("mutually exclusive"));
        e = assertThrows(ParamException.class,
                () -> base.withAccessKey(null).withSecretKey("sk").build());
        assertTrue(e.getMessage().contains("mutually exclusive"));
        e = assertThrows(ParamException.class,
                () -> base.withSecretKey(null).withSessionToken("token").build());
        assertTrue(e.getMessage().contains("mutually exclusive"));
    }

    @Test
    void gcpProviderWithoutSessionTokenFailsLoudly() {
        // a provider that carries no token in Credentials.sessionToken must fail with a
        // clear error, not an opaque "Bearer null" 403 from the storage endpoint
        Provider provider = () -> new Credentials("ak", "sk", null, null);
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("gcp")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withRegion("us-west1")
                .withCredentialsProvider(provider)
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        Exception e = assertThrows(Exception.class,
                () -> client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());
        Throwable root = e;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertTrue(root.getMessage().contains("session token"), root.getMessage());
    }

    @Test
    void sevenArgOverloadReusesProvidedHttpClient() throws Exception {
        MinioStorageClient client = MinioStorageClient.getStorageClient(
                "aws", endpoint(), "ak", "sk", null, "us-west-2", new OkHttpClient());

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertSignedWithAccessKey("ak");
    }

    @Test
    void sevenArgOverloadSignsWithStaticKeys() throws Exception {
        // regression: this overload has no bucketName and must not go through
        // S3ConnectParam.build() validation (VolumeFileManager calls it)
        MinioStorageClient client = MinioStorageClient.getStorageClient(
                "aws", endpoint(), "ak", "sk", null, "us-west-2", null);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertSignedWithAccessKey("ak");
    }

    @Test
    void sevenArgOverloadGcpStaticTokenSendsBearerHeader() throws Exception {
        MinioStorageClient client = MinioStorageClient.getStorageClient(
                "gcp", endpoint(), "ak", "sk", "static-token", "us-west1", null);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertEquals("Bearer static-token", authorizationHeader.get());
    }

    private void assertSignedWithAccessKey(String accessKey) {
        String header = authorizationHeader.get();
        assertNotNull(header, "expected a signed request");
        assertTrue(header.startsWith("AWS4-HMAC-SHA256"), header);
        assertTrue(header.contains("Credential=" + accessKey + "/"), header);
    }

    @Test
    void gcpBearerHeaderComesFromProviderSessionToken() throws Exception {
        AtomicInteger fetchCalls = new AtomicInteger();
        // GCP convention: the bearer token rides in Credentials.sessionToken
        Provider provider = () -> {
            fetchCalls.incrementAndGet();
            return new Credentials("unused", "unused", "fresh-token", null);
        };
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("gcp")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withRegion("us-west1")
                .withCredentialsProvider(provider)
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertEquals("Bearer fresh-token", authorizationHeader.get());
        assertTrue(fetchCalls.get() >= 1);
    }

    @Test
    void gcpStaticSessionTokenStillWorks() throws Exception {
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("gcp")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .withSessionToken("static-token")
                .withRegion("us-west1")
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertEquals("Bearer static-token", authorizationHeader.get());
    }
}
