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
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.DescribeVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.response.volume.ListVolumesResponse;
import io.milvus.bulkwriter.response.volume.VolumeInfo;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class VolumeManagerTest {

    private final Gson gson = new Gson();

    // ========== CreateVolumeRequest Tests ==========

    @Test
    public void testCreateVolumeRequestBuilderManaged() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId("proj-001")
                .regionId("aws-us-west-2")
                .volumeName("my-volume")
                .build();

        assertEquals("proj-001", request.getProjectId());
        assertEquals("aws-us-west-2", request.getRegionId());
        assertEquals("my-volume", request.getVolumeName());
        assertNull(request.getType());
        assertNull(request.getStorageIntegrationId());
        assertNull(request.getPath());
    }

    @Test
    public void testCreateVolumeRequestBuilderExternal() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId("proj-001")
                .regionId("aws-us-west-2")
                .volumeName("ext-volume")
                .type("EXTERNAL")
                .storageIntegrationId("si-xxxx")
                .path("s3://my-bucket/data/")
                .build();

        assertEquals("proj-001", request.getProjectId());
        assertEquals("aws-us-west-2", request.getRegionId());
        assertEquals("ext-volume", request.getVolumeName());
        assertEquals("EXTERNAL", request.getType());
        assertEquals("si-xxxx", request.getStorageIntegrationId());
        assertEquals("s3://my-bucket/data/", request.getPath());
    }

    @Test
    public void testCreateVolumeRequestSetters() {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setProjectId("proj-002");
        request.setRegionId("gcp-us-central1");
        request.setVolumeName("test-vol");
        request.setType("MANAGED");
        request.setStorageIntegrationId("si-yyyy");
        request.setPath("gs://bucket/path/");

        assertEquals("proj-002", request.getProjectId());
        assertEquals("gcp-us-central1", request.getRegionId());
        assertEquals("test-vol", request.getVolumeName());
        assertEquals("MANAGED", request.getType());
        assertEquals("si-yyyy", request.getStorageIntegrationId());
        assertEquals("gs://bucket/path/", request.getPath());
    }

    @Test
    public void testCreateVolumeRequestToString() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId("proj-001")
                .regionId("aws-us-west-2")
                .volumeName("my-volume")
                .type("EXTERNAL")
                .storageIntegrationId("si-xxxx")
                .path("s3://bucket/data/")
                .build();

        String str = request.toString();
        assertTrue(str.contains("proj-001"));
        assertTrue(str.contains("EXTERNAL"));
        assertTrue(str.contains("si-xxxx"));
        assertTrue(str.contains("s3://bucket/data/"));
    }

    @Test
    public void testCreateVolumeRequestSerialization() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId("proj-001")
                .regionId("aws-us-west-2")
                .volumeName("ext-volume")
                .type("EXTERNAL")
                .storageIntegrationId("si-xxxx")
                .path("s3://my-bucket/data/")
                .build();

        String json = gson.toJson(request);
        CreateVolumeRequest deserialized = gson.fromJson(json, CreateVolumeRequest.class);

        assertEquals(request.getProjectId(), deserialized.getProjectId());
        assertEquals(request.getRegionId(), deserialized.getRegionId());
        assertEquals(request.getVolumeName(), deserialized.getVolumeName());
        assertEquals(request.getType(), deserialized.getType());
        assertEquals(request.getStorageIntegrationId(), deserialized.getStorageIntegrationId());
        assertEquals(request.getPath(), deserialized.getPath());
    }

    // ========== ListVolumesRequest Tests ==========

    @Test
    public void testListVolumesRequestBuilder() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId("proj-001")
                .currentPage(1)
                .pageSize(10)
                .build();

        assertEquals("proj-001", request.getProjectId());
        assertEquals(1, request.getCurrentPage());
        assertEquals(10, request.getPageSize());
        assertNull(request.getType());
    }

    @Test
    public void testListVolumesRequestBuilderWithType() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId("proj-001")
                .currentPage(1)
                .pageSize(10)
                .type("EXTERNAL")
                .build();

        assertEquals("proj-001", request.getProjectId());
        assertEquals("EXTERNAL", request.getType());
    }

    @Test
    public void testListVolumesRequestSetters() {
        ListVolumesRequest request = new ListVolumesRequest();
        request.setProjectId("proj-002");
        request.setCurrentPage(2);
        request.setPageSize(20);
        request.setType("MANAGED");

        assertEquals("proj-002", request.getProjectId());
        assertEquals(2, request.getCurrentPage());
        assertEquals(20, request.getPageSize());
        assertEquals("MANAGED", request.getType());
    }

    @Test
    public void testListVolumesRequestToString() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId("proj-001")
                .currentPage(1)
                .pageSize(10)
                .type("EXTERNAL")
                .build();

        String str = request.toString();
        assertTrue(str.contains("proj-001"));
        assertTrue(str.contains("EXTERNAL"));
    }

    @Test
    public void testListVolumesRequestSerialization() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId("proj-001")
                .currentPage(1)
                .pageSize(10)
                .type("MANAGED")
                .build();

        String json = gson.toJson(request);
        ListVolumesRequest deserialized = gson.fromJson(json, ListVolumesRequest.class);

        assertEquals(request.getProjectId(), deserialized.getProjectId());
        assertEquals(request.getCurrentPage(), deserialized.getCurrentPage());
        assertEquals(request.getPageSize(), deserialized.getPageSize());
        assertEquals(request.getType(), deserialized.getType());
    }

    // ========== DescribeVolumeRequest Tests ==========

    @Test
    public void testDescribeVolumeRequestBuilder() {
        DescribeVolumeRequest request = DescribeVolumeRequest.builder()
                .volumeName("my-volume")
                .build();

        assertEquals("my-volume", request.getVolumeName());
    }

    @Test
    public void testDescribeVolumeRequestSetters() {
        DescribeVolumeRequest request = new DescribeVolumeRequest();
        request.setVolumeName("test-volume");

        assertEquals("test-volume", request.getVolumeName());
    }

    @Test
    public void testDescribeVolumeRequestConstructor() {
        DescribeVolumeRequest request = new DescribeVolumeRequest("my-volume");

        assertEquals("my-volume", request.getVolumeName());
    }

    @Test
    public void testDescribeVolumeRequestToString() {
        DescribeVolumeRequest request = DescribeVolumeRequest.builder()
                .volumeName("my-volume")
                .build();

        String str = request.toString();
        assertTrue(str.contains("my-volume"));
    }

    @Test
    public void testDescribeVolumeRequestSerialization() {
        DescribeVolumeRequest request = DescribeVolumeRequest.builder()
                .volumeName("my-volume")
                .build();

        String json = gson.toJson(request);
        DescribeVolumeRequest deserialized = gson.fromJson(json, DescribeVolumeRequest.class);

        assertEquals(request.getVolumeName(), deserialized.getVolumeName());
    }

    // ========== DeleteVolumeRequest Tests ==========

    @Test
    public void testDeleteVolumeRequestBuilder() {
        DeleteVolumeRequest request = DeleteVolumeRequest.builder()
                .volumeName("my-volume")
                .build();

        assertEquals("my-volume", request.getVolumeName());
    }

    @Test
    public void testDeleteVolumeRequestToString() {
        DeleteVolumeRequest request = DeleteVolumeRequest.builder()
                .volumeName("my-volume")
                .build();

        String str = request.toString();
        assertTrue(str.contains("my-volume"));
    }

    // ========== VolumeInfo Tests ==========

    @Test
    public void testVolumeInfoBuilderAllFields() {
        VolumeInfo info = VolumeInfo.builder()
                .volumeName("ext-volume")
                .type("EXTERNAL")
                .regionId("aws-us-west-2")
                .storageIntegrationId("si-xxxx")
                .path("s3://my-bucket/data/")
                .status("RUNNING")
                .createTime("2024-04-15T12:00:00Z")
                .build();

        assertEquals("ext-volume", info.getVolumeName());
        assertEquals("EXTERNAL", info.getType());
        assertEquals("aws-us-west-2", info.getRegionId());
        assertEquals("si-xxxx", info.getStorageIntegrationId());
        assertEquals("s3://my-bucket/data/", info.getPath());
        assertEquals("RUNNING", info.getStatus());
        assertEquals("2024-04-15T12:00:00Z", info.getCreateTime());
    }

    @Test
    public void testVolumeInfoBuilderManagedVolume() {
        VolumeInfo info = VolumeInfo.builder()
                .volumeName("managed-vol")
                .type("MANAGED")
                .regionId("aws-us-west-2")
                .status("RUNNING")
                .createTime("2024-04-15T12:00:00Z")
                .build();

        assertEquals("managed-vol", info.getVolumeName());
        assertEquals("MANAGED", info.getType());
        assertNull(info.getStorageIntegrationId());
        assertNull(info.getPath());
    }

    @Test
    public void testVolumeInfoSetters() {
        VolumeInfo info = new VolumeInfo();
        info.setVolumeName("test-vol");
        info.setType("EXTERNAL");
        info.setRegionId("gcp-us-central1");
        info.setStorageIntegrationId("si-yyyy");
        info.setPath("gs://bucket/path/");
        info.setStatus("FROZEN");
        info.setCreateTime("2024-04-16T00:00:00Z");

        assertEquals("test-vol", info.getVolumeName());
        assertEquals("EXTERNAL", info.getType());
        assertEquals("gcp-us-central1", info.getRegionId());
        assertEquals("si-yyyy", info.getStorageIntegrationId());
        assertEquals("gs://bucket/path/", info.getPath());
        assertEquals("FROZEN", info.getStatus());
        assertEquals("2024-04-16T00:00:00Z", info.getCreateTime());
    }

    @Test
    public void testVolumeInfoToString() {
        VolumeInfo info = VolumeInfo.builder()
                .volumeName("ext-volume")
                .type("EXTERNAL")
                .regionId("aws-us-west-2")
                .status("RUNNING")
                .createTime("2024-04-15T12:00:00Z")
                .build();

        String str = info.toString();
        assertTrue(str.contains("ext-volume"));
        assertTrue(str.contains("EXTERNAL"));
        assertTrue(str.contains("aws-us-west-2"));
        assertTrue(str.contains("RUNNING"));
    }

    @Test
    public void testVolumeInfoSerialization() {
        VolumeInfo info = VolumeInfo.builder()
                .volumeName("ext-volume")
                .type("EXTERNAL")
                .regionId("aws-us-west-2")
                .storageIntegrationId("si-xxxx")
                .path("s3://my-bucket/data/")
                .status("RUNNING")
                .createTime("2024-04-15T12:00:00Z")
                .build();

        String json = gson.toJson(info);
        VolumeInfo deserialized = gson.fromJson(json, VolumeInfo.class);

        assertEquals(info.getVolumeName(), deserialized.getVolumeName());
        assertEquals(info.getType(), deserialized.getType());
        assertEquals(info.getRegionId(), deserialized.getRegionId());
        assertEquals(info.getStorageIntegrationId(), deserialized.getStorageIntegrationId());
        assertEquals(info.getPath(), deserialized.getPath());
        assertEquals(info.getStatus(), deserialized.getStatus());
        assertEquals(info.getCreateTime(), deserialized.getCreateTime());
    }

    @Test
    public void testVolumeInfoDeserializationFromApiResponse() {
        String apiJson = "{\"volumeName\":\"ext-volume\",\"type\":\"EXTERNAL\"," +
                "\"regionId\":\"aws-us-west-2\",\"storageIntegrationId\":\"si-xxxx\"," +
                "\"path\":\"s3://my-bucket/data/\",\"status\":\"RUNNING\"," +
                "\"createTime\":\"2024-04-15T12:00:00Z\"}";

        VolumeInfo info = gson.fromJson(apiJson, VolumeInfo.class);

        assertEquals("ext-volume", info.getVolumeName());
        assertEquals("EXTERNAL", info.getType());
        assertEquals("aws-us-west-2", info.getRegionId());
        assertEquals("si-xxxx", info.getStorageIntegrationId());
        assertEquals("s3://my-bucket/data/", info.getPath());
        assertEquals("RUNNING", info.getStatus());
        assertEquals("2024-04-15T12:00:00Z", info.getCreateTime());
    }

    @Test
    public void testVolumeInfoDeserializationManagedVolume() {
        String apiJson = "{\"volumeName\":\"managed-vol\",\"type\":\"MANAGED\"," +
                "\"regionId\":\"aws-us-west-2\",\"status\":\"RUNNING\"," +
                "\"createTime\":\"2024-04-15T12:00:00Z\"}";

        VolumeInfo info = gson.fromJson(apiJson, VolumeInfo.class);

        assertEquals("managed-vol", info.getVolumeName());
        assertEquals("MANAGED", info.getType());
        assertNull(info.getStorageIntegrationId());
        assertNull(info.getPath());
    }

    // ========== ListVolumesResponse Tests ==========

    @Test
    public void testListVolumesResponseBuilder() {
        VolumeInfo vol1 = VolumeInfo.builder().volumeName("vol-1").type("MANAGED").build();
        VolumeInfo vol2 = VolumeInfo.builder().volumeName("vol-2").type("EXTERNAL").build();

        ListVolumesResponse response = ListVolumesResponse.builder()
                .count(2)
                .currentPage(1)
                .pageSize(10)
                .volumes(Arrays.asList(vol1, vol2))
                .build();

        assertEquals(2, response.getCount());
        assertEquals(1, response.getCurrentPage());
        assertEquals(10, response.getPageSize());
        assertEquals(2, response.getVolumes().size());
        assertEquals("vol-1", response.getVolumes().get(0).getVolumeName());
        assertEquals("MANAGED", response.getVolumes().get(0).getType());
        assertEquals("vol-2", response.getVolumes().get(1).getVolumeName());
        assertEquals("EXTERNAL", response.getVolumes().get(1).getType());
    }

    @Test
    public void testListVolumesResponseDeserialization() {
        String json = "{\"count\":2,\"currentPage\":1,\"pageSize\":10," +
                "\"volumes\":[{\"volumeName\":\"vol-1\",\"type\":\"MANAGED\"}," +
                "{\"volumeName\":\"vol-2\",\"type\":\"EXTERNAL\"}]}";

        ListVolumesResponse response = gson.fromJson(json, ListVolumesResponse.class);

        assertEquals(2, response.getCount());
        assertEquals(1, response.getCurrentPage());
        assertEquals(10, response.getPageSize());
        assertEquals(2, response.getVolumes().size());
        assertEquals("MANAGED", response.getVolumes().get(0).getType());
        assertEquals("EXTERNAL", response.getVolumes().get(1).getType());
    }
}
