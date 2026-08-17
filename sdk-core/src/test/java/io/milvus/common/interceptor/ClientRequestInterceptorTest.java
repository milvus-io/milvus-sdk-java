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

package io.milvus.common.interceptor;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ClientRequestInterceptorTest {
    private static final Metadata.Key<String> CLIENT_REQUEST_ID_HEADER =
            Metadata.Key.of("client_request_id", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    void explicitRequestIdOverridesExecutingThread() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set("executing-thread");

        String captured = intercept(requestId, CallOptions.DEFAULT.withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "caller-thread"));

        assertEquals("caller-thread", captured);
    }

    @Test
    void explicitEmptyRequestIdSuppressesExecutingThread() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set("executing-thread");

        String captured = intercept(requestId, CallOptions.DEFAULT.withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, ""));

        assertNull(captured);
    }

    @Test
    void fallsBackToThreadLocalWithoutExplicitRequestId() {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        requestId.set("executing-thread");

        assertEquals("executing-thread", intercept(requestId, CallOptions.DEFAULT));
    }

    private String intercept(ThreadLocal<String> requestId, CallOptions callOptions) {
        String[] captured = {null};
        ClientRequestInterceptor interceptor = new ClientRequestInterceptor(requestId);
        ClientCall<Object, Object> call = interceptor.interceptCall(
                method(), callOptions, channel(captured));
        call.start(new ClientCall.Listener<Object>() {}, new Metadata());
        return captured[0];
    }

    private Channel channel(String[] captured) {
        return new Channel() {
            @Override
            public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                    MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<RequestT, ResponseT>() {
                    @Override
                    public void start(Listener<ResponseT> responseListener, Metadata headers) {
                        captured[0] = headers.get(CLIENT_REQUEST_ID_HEADER);
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void halfClose() {}

                    @Override
                    public void sendMessage(RequestT message) {}
                };
            }

            @Override
            public String authority() {
                return "test";
            }
        };
    }

    private MethodDescriptor<Object, Object> method() {
        return MethodDescriptor.newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test/method")
                .setRequestMarshaller(new NoopMarshaller())
                .setResponseMarshaller(new NoopMarshaller())
                .build();
    }

    private static class NoopMarshaller implements MethodDescriptor.Marshaller<Object> {
        @Override
        public InputStream stream(Object value) {
            return null;
        }

        @Override
        public Object parse(InputStream stream) {
            return null;
        }
    }
}
