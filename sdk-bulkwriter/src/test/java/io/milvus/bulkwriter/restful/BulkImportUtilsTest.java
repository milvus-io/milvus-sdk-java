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

package io.milvus.bulkwriter.restful;

import com.google.gson.JsonObject;
import io.milvus.bulkwriter.request.describe.CloudDescribeImportRequest;
import io.milvus.bulkwriter.request.describe.MilvusDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.bulkwriter.request.list.CloudListImportJobsRequest;
import io.milvus.common.utils.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class BulkImportUtilsTest {
    @Test
    void testBulkImport() throws IOException {
        AtomicReference<String> requestPath = new AtomicReference<>("");
        AtomicReference<String> requestBody = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/vectordb/jobs/import/create", exchange ->
                handleRequest(exchange, requestPath, requestBody));
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            Map<String, Object> options = new HashMap<>();
            options.put("sep", "|");
            MilvusImportRequest request = MilvusImportRequest.builder()
                    .apiKey("user:password")
                    .dbName("db1")
                    .collectionName("coll1")
                    .partitionName("part1")
                    .files(Arrays.asList(Collections.singletonList("1.parquet"), Collections.singletonList("2.parquet")))
                    .options(options)
                    .build();

            String response = BulkImportUtils.bulkImport(url, request);
            Assertions.assertNotNull(response);
            Assertions.assertEquals("/v2/vectordb/jobs/import/create", requestPath.get());

            JsonObject body = JsonUtils.parseFromString(requestBody.get());
            Assertions.assertEquals("user:password", body.get("apiKey").getAsString());
            Assertions.assertEquals("db1", body.get("dbName").getAsString());
            Assertions.assertEquals("coll1", body.get("collectionName").getAsString());
            Assertions.assertEquals("part1", body.get("partitionName").getAsString());
            Assertions.assertEquals(2, body.getAsJsonArray("files").size());
            Assertions.assertEquals("|", body.getAsJsonObject("options").get("sep").getAsString());
            Assertions.assertEquals("java", body.getAsJsonObject("options").get("sdk").getAsString());
            Assertions.assertEquals("bulkWriter", body.getAsJsonObject("options").get("scene").getAsString());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testGetImportProgress() throws IOException {
        AtomicReference<String> requestPath = new AtomicReference<>("");
        AtomicReference<String> requestBody = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/vectordb/jobs/import/describe", exchange ->
                handleRequest(exchange, requestPath, requestBody));
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            CloudDescribeImportRequest request = CloudDescribeImportRequest.builder()
                    .apiKey("token")
                    .clusterId("cluster-0")
                    .projectId("project-0")
                    .regionId("region-0")
                    .jobId("job-0")
                    .build();

            String response = BulkImportUtils.getImportProgress(url, request);
            Assertions.assertNotNull(response);
            Assertions.assertEquals("/v2/vectordb/jobs/import/describe", requestPath.get());

            JsonObject body = JsonUtils.parseFromString(requestBody.get());
            Assertions.assertEquals("cluster-0", body.get("clusterId").getAsString());
            Assertions.assertEquals("project-0", body.get("projectId").getAsString());
            Assertions.assertEquals("region-0", body.get("regionId").getAsString());
            Assertions.assertEquals("job-0", body.get("jobId").getAsString());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testListImportJobs() throws IOException {
        AtomicReference<String> requestPath = new AtomicReference<>("");
        AtomicReference<String> requestBody = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/vectordb/jobs/import/list", exchange ->
                handleRequest(exchange, requestPath, requestBody));
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            CloudListImportJobsRequest request = CloudListImportJobsRequest.builder()
                    .apiKey("token")
                    .clusterId("cluster-2")
                    .projectId("project-2")
                    .regionId("region-2")
                    .pageSize(20)
                    .currentPage(3)
                    .build();

            String response = BulkImportUtils.listImportJobs(url, request);
            Assertions.assertNotNull(response);
            Assertions.assertEquals("/v2/vectordb/jobs/import/list", requestPath.get());

            JsonObject body = JsonUtils.parseFromString(requestBody.get());
            Assertions.assertEquals("cluster-2", body.get("clusterId").getAsString());
            Assertions.assertEquals("project-2", body.get("projectId").getAsString());
            Assertions.assertEquals("region-2", body.get("regionId").getAsString());
            Assertions.assertEquals(20, body.get("pageSize").getAsInt());
            Assertions.assertEquals(3, body.get("currentPage").getAsInt());
            Assertions.assertEquals("java", body.getAsJsonObject("options").get("sdk").getAsString());
            Assertions.assertEquals("bulkWriter", body.getAsJsonObject("options").get("scene").getAsString());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testCommitImport() throws IOException {
        AtomicReference<String> requestPath = new AtomicReference<>("");
        AtomicReference<String> requestBody = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/vectordb/jobs/import/commit", exchange ->
                handleRequest(exchange, requestPath, requestBody));
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            CloudDescribeImportRequest request = CloudDescribeImportRequest.builder()
                    .apiKey("token")
                    .clusterId("cluster-1")
                    .projectId("project-1")
                    .regionId("region-1")
                    .jobId("job-1")
                    .build();

            String response = BulkImportUtils.commitImport(url, request);
            Assertions.assertNotNull(response);
            Assertions.assertEquals("/v2/vectordb/jobs/import/commit", requestPath.get());

            JsonObject body = JsonUtils.parseFromString(requestBody.get());
            Assertions.assertEquals("token", body.get("apiKey").getAsString());
            Assertions.assertEquals("cluster-1", body.get("clusterId").getAsString());
            Assertions.assertEquals("project-1", body.get("projectId").getAsString());
            Assertions.assertEquals("region-1", body.get("regionId").getAsString());
            Assertions.assertEquals("job-1", body.get("jobId").getAsString());
            Assertions.assertEquals("java", body.getAsJsonObject("options").get("sdk").getAsString());
            Assertions.assertEquals("bulkWriter", body.getAsJsonObject("options").get("scene").getAsString());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testAbortImport() throws IOException {
        AtomicReference<String> requestPath = new AtomicReference<>("");
        AtomicReference<String> requestBody = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/vectordb/jobs/import/abort", exchange ->
                handleRequest(exchange, requestPath, requestBody));
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            MilvusDescribeImportRequest request = MilvusDescribeImportRequest.builder()
                    .apiKey("user:password")
                    .jobId("job-2")
                    .build();

            String response = BulkImportUtils.abortImport(url, request);
            Assertions.assertNotNull(response);
            Assertions.assertEquals("/v2/vectordb/jobs/import/abort", requestPath.get());

            JsonObject body = JsonUtils.parseFromString(requestBody.get());
            Assertions.assertEquals("user:password", body.get("apiKey").getAsString());
            Assertions.assertEquals("job-2", body.get("jobId").getAsString());
            Assertions.assertEquals("java", body.getAsJsonObject("options").get("sdk").getAsString());
            Assertions.assertEquals("bulkWriter", body.getAsJsonObject("options").get("scene").getAsString());
        } finally {
            server.stop(0);
        }
    }

    private static void handleRequest(HttpExchange exchange,
                                      AtomicReference<String> requestPath,
                                      AtomicReference<String> requestBody) throws IOException {
        requestPath.set(exchange.getRequestURI().getPath());
        requestBody.set(readAll(exchange.getRequestBody()));
        byte[] response = "{\"code\":0,\"message\":\"success\",\"data\":{}}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(response);
        }
    }

    private static String readAll(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, read);
        }
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }
}
