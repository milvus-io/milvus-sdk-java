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

package io.milvus.integration.v2.utils;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.milvus.grpc.ConnectRequest;
import io.milvus.grpc.ConnectResponse;
import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.telemetry.TelemetryConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class ZstdCompressionTest {
    @Test
    void compressesSyncAndAsyncMessagesAcrossDatabaseChangesOnlyWhenEnabled() throws Exception {
        CountingCodec serverCodec = new CountingCodec();
        CompressorRegistry compressors = CompressorRegistry.newEmptyInstance();
        compressors.register(Codec.Identity.NONE);
        compressors.register(serverCodec);
        Server server = ServerBuilder.forPort(0).compressorRegistry(compressors)
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance().with(serverCodec, true))
                .addService(new TestMilvusService() {
                    @Override
                    public void query(QueryRequest req, StreamObserver<QueryResults> observer) {
                        ((ServerCallStreamObserver<?>) observer).setCompression("zstd");
                        observer.onNext(QueryResults.newBuilder().setSessionTs(123).build());
                        observer.onCompleted();
                    }
                }).build().start();
        try {
            QueryReq query = QueryReq.builder().collectionName("books").filter("id > 0").build();
            MilvusClientV2 plain = new MilvusClientV2(clientConfig(server).build());
            try {
                assertEquals(123L, plain.query(query).getSessionTs());
                assertEquals(0, serverCodec.decoded.get());
                assertEquals(0, serverCodec.encoded.get());
            } finally {
                plain.close();
            }

            MilvusClientV2 client = new MilvusClientV2(clientConfig(server).enableZstdCompression(true).build());
            try {
                int decoded = serverCodec.decoded.get();
                assertEquals(123L, client.query(query).getSessionTs());
                assertEquals(decoded + 1, serverCodec.decoded.get());
                assertEquals(1, serverCodec.encoded.get());

                client.useDatabase("other");
                decoded = serverCodec.decoded.get();
                assertEquals(123L, client.queryAsync(query).get(10, TimeUnit.SECONDS).getSessionTs());
                assertEquals(decoded + 1, serverCodec.decoded.get());
                assertEquals(2, serverCodec.encoded.get());
            } finally {
                client.close();
            }
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void peerWithoutZstdAcceptsDefaultClientAndRejectsOptIn() throws Exception {
        Server server = ServerBuilder.forPort(0).addService(new TestMilvusService()).build().start();
        try {
            MilvusClientV2 plain = new MilvusClientV2(clientConfig(server).build());
            try {
                assertEquals(Collections.singletonList("other"), plain.listDatabases().getDatabaseNames());
            } finally {
                plain.close();
            }

            StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                    () -> new MilvusClientV2(clientConfig(server).enableZstdCompression(true).build()));
            assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
            assertTrue(error.getMessage().contains("zstd"), error.getMessage());
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static ConnectConfig.ConnectConfigBuilder clientConfig(Server server) {
        return ConnectConfig.builder().uri("http://127.0.0.1:" + server.getPort())
                .telemetryConfig(TelemetryConfig.builder().enabled(false).build());
    }

    private static class TestMilvusService extends MilvusServiceGrpc.MilvusServiceImplBase {
        @Override
        public void connect(ConnectRequest req, StreamObserver<ConnectResponse> observer) {
            observer.onNext(ConnectResponse.newBuilder().setIdentifier(1).build());
            observer.onCompleted();
        }

        @Override
        public void listDatabases(ListDatabasesRequest req, StreamObserver<ListDatabasesResponse> observer) {
            observer.onNext(ListDatabasesResponse.newBuilder().addDbNames("other").build());
            observer.onCompleted();
        }
    }

    // Count actual per-message encoding and decoding independently on the peer.
    private static final class CountingCodec implements Codec {
        final AtomicInteger encoded = new AtomicInteger();
        final AtomicInteger decoded = new AtomicInteger();

        @Override
        public String getMessageEncoding() {
            return "zstd";
        }

        @Override
        public OutputStream compress(OutputStream output) throws IOException {
            encoded.incrementAndGet();
            return new ZstdOutputStream(output);
        }

        @Override
        public InputStream decompress(InputStream input) throws IOException {
            decoded.incrementAndGet();
            return new ZstdInputStream(input);
        }
    }
}
