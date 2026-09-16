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

package io.milvus.bulkwriter;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.DescribeVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.response.volume.ListVolumesResponse;
import io.milvus.bulkwriter.response.volume.VolumeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
public class VolumeManagerTest {

    private HttpServer server;
    private VolumeManager volumeManager;
    private final AtomicReference<String> lastRequestPath = new AtomicReference<>("");
    private final AtomicReference<String> lastRequestBody = new AtomicReference<>("");
    private final AtomicReference<String> lastMethod = new AtomicReference<>("");

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v2/volumes", this::handleVolumeRequest);
        server.start();

        String url = "http://127.0.0.1:" + server.getAddress().getPort();
        VolumeManagerParam param = VolumeManagerParam.newBuilder()
                .withCloudEndpoint(url)
                .withApiKey("test-api-key")
                .build();
        volumeManager = new VolumeManager(param);
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private void handleVolumeRequest(HttpExchange exchange) throws IOException {
        lastRequestPath.set(exchange.getRequestURI().getPath());
        lastMethod.set(exchange.getRequestMethod());
        lastRequestBody.set(readAll(exchange.getRequestBody()));

        String path = exchange.getRequestURI().getPath();
        String data = "null";
        if (path.equals("/v2/volumes/describe-volume")) {
            data = "{\"volumeName\":\"describe-volume\",\"type\":\"MANAGED\",\"regionId\":\"aws-us-west-2\",\"status\":\"RUNNING\"}";
        } else if (path.equals("/v2/volumes") && "GET".equals(exchange.getRequestMethod())) {
            data = "{\"count\":2,\"currentPage\":1,\"pageSize\":10,\"volumes\":[{\"volumeName\":\"vol-1\",\"type\":\"MANAGED\"},{\"volumeName\":\"vol-2\",\"type\":\"EXTERNAL\"}]}";
        }
        String response = String.format("{\"code\":0,\"message\":\"success\",\"data\":%s}", data);
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private static String readAll(InputStream inputStream) throws IOException {
        StringBuilder builder = new StringBuilder();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            builder.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
        }
        return builder.toString();
    }

    @Test
    void testCreateVolume() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId("proj-001")
                .regionId("aws-us-west-2")
                .volumeName("my-volume")
                .build();

        volumeManager.createVolume(request);

        assertEquals("POST", lastMethod.get());
        assertEquals("/v2/volumes/create", lastRequestPath.get());
        Gson gson = new Gson();
        io.milvus.bulkwriter.request.volume.CreateVolumeRequest body =
                gson.fromJson(lastRequestBody.get(), io.milvus.bulkwriter.request.volume.CreateVolumeRequest.class);
        assertEquals("proj-001", body.getProjectId());
        assertEquals("aws-us-west-2", body.getRegionId());
        assertEquals("my-volume", body.getVolumeName());
    }

    @Test
    void testDescribeVolume() {
        DescribeVolumeRequest request = DescribeVolumeRequest.builder()
                .volumeName("describe-volume")
                .build();

        VolumeInfo info = volumeManager.describeVolume(request);

        assertEquals("GET", lastMethod.get());
        assertEquals("/v2/volumes/describe-volume", lastRequestPath.get());
        assertNotNull(info);
        assertEquals("describe-volume", info.getVolumeName());
        assertEquals("MANAGED", info.getType());
        assertEquals("RUNNING", info.getStatus());
    }

    @Test
    void testDeleteVolume() {
        DeleteVolumeRequest request = DeleteVolumeRequest.builder()
                .volumeName("delete-volume")
                .build();

        volumeManager.deleteVolume(request);

        assertEquals("DELETE", lastMethod.get());
        assertEquals("/v2/volumes/delete-volume", lastRequestPath.get());
    }

    @Test
    void testListVolumes() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId("proj-001")
                .currentPage(1)
                .pageSize(10)
                .build();

        ListVolumesResponse response = volumeManager.listVolumes(request);

        assertEquals("GET", lastMethod.get());
        assertEquals("/v2/volumes", lastRequestPath.get());
        assertNotNull(response);
        assertEquals(2, response.getCount());
        assertEquals(2, response.getVolumes().size());
        assertEquals("vol-1", response.getVolumes().get(0).getVolumeName());
        assertEquals("EXTERNAL", response.getVolumes().get(1).getType());
    }

    @Test
    void testVolumeManagerParamValidation() {
        assertThrows(io.milvus.exception.ParamException.class,
                () -> VolumeManagerParam.newBuilder().build());
        assertThrows(io.milvus.exception.ParamException.class,
                () -> VolumeManagerParam.newBuilder().withCloudEndpoint("https://api.cloud.zilliz.com").build());
    }

    @Test
    void testVolumeManagerConstructorAndGetters() {
        VolumeManagerParam param = VolumeManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("api-key")
                .build();
        VolumeManager manager = new VolumeManager(param);
        Assertions.assertNotNull(manager);
    }

    @Test
    void testListVolumesResponseSerializationRoundTrip() {
        VolumeInfo v1 = VolumeInfo.builder().volumeName("vol-1").type("MANAGED").build();
        VolumeInfo v2 = VolumeInfo.builder().volumeName("vol-2").type("EXTERNAL").build();
        ListVolumesResponse response = ListVolumesResponse.builder()
                .count(2).currentPage(1).pageSize(10)
                .volumes(Arrays.asList(v1, v2))
                .build();
        Gson gson = new Gson();
        ListVolumesResponse roundTrip = gson.fromJson(gson.toJson(response), ListVolumesResponse.class);
        assertEquals(2, roundTrip.getCount());
        assertEquals(2, roundTrip.getVolumes().size());
        assertEquals("vol-2", roundTrip.getVolumes().get(1).getVolumeName());
    }
}
