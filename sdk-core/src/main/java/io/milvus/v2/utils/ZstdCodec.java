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

package io.milvus.v2.utils;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Zstandard framing for individual gRPC messages. */
final class ZstdCodec implements Codec {
    // Matches the 8 MiB maximum decode window used by the Milvus proxy.
    private static final int MAX_WINDOW_LOG = 23;

    static void configure(ManagedChannelBuilder<?> builder) {
        ZstdCodec zstd = new ZstdCodec();
        CompressorRegistry compressors = CompressorRegistry.newEmptyInstance();
        compressors.register(Codec.Identity.NONE);
        compressors.register(new Codec.Gzip());
        compressors.register(zstd);
        builder.compressorRegistry(compressors)
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance().with(zstd, true))
                .intercept(new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
                        return next.newCall(method, options.withCompression(zstd.getMessageEncoding()));
                    }
                });
    }

    @Override
    public String getMessageEncoding() {
        return "zstd";
    }

    @Override
    public OutputStream compress(OutputStream output) throws IOException {
        return new ZstdOutputStream(output, 1);
    }

    @Override
    public InputStream decompress(InputStream input) throws IOException {
        return new ZstdInputStream(input).setLongMax(MAX_WINDOW_LOG);
    }
}
