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

package io.milvus.integration.v1.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.support.server.MockMilvusServer;
import io.milvus.support.server.MockMilvusServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class MilvusServiceClientConnectTest {
    private final int testPort = 53019;
    private MockMilvusServerImpl mockServerImpl;

    private MockMilvusServer startServer() {
        mockServerImpl = new MockMilvusServerImpl();
        MockMilvusServer mockServer = new MockMilvusServer(testPort, mockServerImpl);
        mockServer.start();
        return mockServer;
    }

    private MilvusClient startClient() {
        String testHost = "localhost";
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(testHost)
                .withPort(testPort)
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(2)
                .build();
        return new MilvusServiceClient(connectParam).withRetry(retryParam);
    }

    @SuppressWarnings("unchecked")
    private <T, P> void invokeFunc(Method testFunc, MilvusClient client, T param, int ret, boolean equalRet) {
        try {
            R<P> resp = (R<P>) testFunc.invoke(client, param);
            if (equalRet) {
                assertEquals(ret, resp.getStatus());
            } else {
                assertNotEquals(ret, resp.getStatus());
            }
        } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        }
    }

    private <T, P> void testFuncByName(String funcName, T param) {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();
        try {
            Class<?> clientClass = MilvusServiceClient.class;
            Method testFunc = clientClass.getMethod(funcName, param.getClass());

            // test return ok with correct input
            invokeFunc(testFunc, client, param, R.Status.Success.getCode(), true);

            // stop mock server
            server.stop();

            // test return error without server
            invokeFunc(testFunc, client, param, R.Status.Success.getCode(), false);

            // test return error when client channel is shutdown
            client.close();
            invokeFunc(testFunc, client, param, R.Status.ClientNotConnected.getCode(), true);
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        } finally {
            server.stop();
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private <T, P> void testAsyncFuncByName(String funcName, T param) {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        try {
            Class<?> clientClass = MilvusServiceClient.class;
            Method testFunc = clientClass.getMethod(funcName, param.getClass());

            // test return ok with correct input
            try {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertEquals(R.Status.Success.getCode(), response.getStatus());
            } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException |
                     InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                fail();
            }

            // stop mock server
            server.stop();

            // test return error without server
            assertThrows(ExecutionException.class, () -> {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertNotEquals(R.Status.Success.getCode(), response.getStatus());
            });

            // test return error when client channel is shutdown
            client.close();
            try {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertEquals(R.Status.ClientNotConnected.getCode(), response.getStatus());
            } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException |
                     InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                fail();
            }
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        } finally {
            server.stop();
            client.close();
        }
    }

    @Test
    void connectParam() {
        System.out.println(System.getProperty("os.name"));
        System.out.println(System.getProperty("os.arch"));

        String host = "dummyHost";
        int port = 100;
        long connectTimeoutMs = 1;
        long keepAliveTimeMs = 2;
        long keepAliveTimeoutMs = 3;
        long idleTimeoutMs = 5;
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(host)
                .withPort(port)
                .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(true)
                .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
        System.out.println(connectParam.toString());

        assertEquals(0, host.compareTo(connectParam.getHost()));
        assertEquals(port, connectParam.getPort());
        assertEquals(connectTimeoutMs, connectParam.getConnectTimeoutMs());
        assertEquals(keepAliveTimeMs, connectParam.getKeepAliveTimeMs());
        assertEquals(keepAliveTimeoutMs, connectParam.getKeepAliveTimeoutMs());
        assertTrue(connectParam.isKeepAliveWithoutCalls());
        assertEquals(idleTimeoutMs, connectParam.getIdleTimeoutMs());

        assertThrows(ParamException.class, () ->
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(0xFFFF + 1)
                        .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .withConnectTimeout(-1, TimeUnit.MILLISECONDS)
                        .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTime(-1, TimeUnit.MILLISECONDS)
                        .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTimeout(-1, TimeUnit.NANOSECONDS)
                        .keepAliveWithoutCalls(true)
                        .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                        .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .withIdleTimeout(-1, TimeUnit.MILLISECONDS)
                        .build()
        );
    }

    @Test
    void connectParamDefaults() {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("dummyHost")
                .withPort(19530)
                .build();
        assertEquals(10000, connectParam.getKeepAliveTimeMs());
        assertEquals(5000, connectParam.getKeepAliveTimeoutMs());
        assertTrue(connectParam.isKeepAliveWithoutCalls());
    }

    @Test
    void testConnect() {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(testPort)
                .withConnectTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(2)
                .build();

        Exception e = assertThrows(RuntimeException.class, () -> {
            MilvusClient client = new MilvusServiceClient(connectParam).withRetry(retryParam);
        });
        assertTrue(e.getMessage().contains("DEADLINE_EXCEEDED"));

        MockMilvusServer server = startServer();
        String dbName = "base";
        String reason = "database not found[database=" + dbName + "]";
        mockServerImpl.setConnectResponse(ConnectResponse.newBuilder()
                .setStatus(Status.newBuilder().setCode(800).setReason(reason).build()).build());

        e = assertThrows(RuntimeException.class, () -> {
            MilvusClient client = new MilvusServiceClient(connectParam).withRetry(retryParam);
        });
        assertTrue(e.getMessage().contains(reason));

        server.stop();
    }

    @Test
    void testConnectWithClientRequestId() {
        ThreadLocal<String> clientRequestId = new ThreadLocal<>();
        clientRequestId.set("req1");
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(testPort)
                .withConnectTimeout(10000, TimeUnit.MILLISECONDS)
                .withClientRequestId(clientRequestId)
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(2)
                .build();

        MockMilvusServer server = startServer();
        MilvusServiceClient client = new MilvusServiceClient(connectParam);
        client.withRetry(retryParam);
        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();
        R<DescribeCollectionResponse> response = client.describeCollection(param);

        assertEquals(0, (int) response.getStatus());

        server.stop();
    }
}
