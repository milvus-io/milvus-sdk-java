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
import io.milvus.bulkwriter.model.UploadProgress;
import io.milvus.bulkwriter.request.volume.ApplyVolumeRequest;
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.DescribeVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.request.volume.UploadFilesRequest;
import io.milvus.bulkwriter.response.ApplyVolumeResponse;
import io.milvus.bulkwriter.response.volume.ListVolumesResponse;
import io.milvus.bulkwriter.response.volume.VolumeInfo;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.exception.ParamException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class VolumeManagerTest {

    private final Gson gson = new Gson();

    @Test
    public void testUploadFilesRequestDefaultUploadOptions() {
        UploadFilesRequest request = UploadFilesRequest.builder()
                .sourceFilePath("/tmp/data")
                .targetVolumePath("data/")
                .build();

        assertEquals(5, request.getUploadConcurrency());
        assertEquals(5, request.getMaxRetries());
        assertEquals(5000L, request.getRetryIntervalMillis());
        assertNull(request.getProgressListener());
        assertEquals(0L, request.getPartSizeBytes());
    }

    @Test
    public void testUploadFilesRequestCustomUploadOptions() {
        UploadFilesRequest.ProgressListener progressListener = progress -> {
        };
        UploadFilesRequest request = UploadFilesRequest.builder()
                .sourceFilePath("/tmp/data")
                .targetVolumePath("data/")
                .uploadConcurrency(3)
                .maxRetries(2)
                .retryIntervalMillis(100L)
                .progressListener(progressListener)
                .partSizeBytes(16L * 1024L * 1024L)
                .build();

        assertEquals(3, request.getUploadConcurrency());
        assertEquals(2, request.getMaxRetries());
        assertEquals(100L, request.getRetryIntervalMillis());
        assertSame(progressListener, request.getProgressListener());
        assertEquals(16L * 1024L * 1024L, request.getPartSizeBytes());
    }

    @Test
    public void testConcurrentUploadFilesUseRequestLocalVolumeState() throws Exception {
        Path fileA = Files.createTempFile("volume-upload-a", ".txt");
        Path fileB = Files.createTempFile("volume-upload-b", ".txt");
        Files.write(fileA, Collections.singletonList("a"));
        Files.write(fileB, Collections.singletonList("b"));
        CyclicBarrier barrier = new CyclicBarrier(2);
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(barrier);

        try {
            CompletableFuture<?> futureA = manager.uploadFilesAsync(UploadFilesRequest.builder()
                    .sourceFilePath(fileA.toString())
                    .targetVolumePath("a/")
                    .uploadConcurrency(1)
                    .build());
            CompletableFuture<?> futureB = manager.uploadFilesAsync(UploadFilesRequest.builder()
                    .sourceFilePath(fileB.toString())
                    .targetVolumePath("b/")
                    .uploadConcurrency(1)
                    .build());

            futureA.get();
            futureB.get();

            assertEquals(Collections.singletonList("prefix-a/a/" + fileA.getFileName()),
                    manager.uploadsByBucket.get("bucket-a"));
            assertEquals(Collections.singletonList("prefix-b/b/" + fileB.getFileName()),
                    manager.uploadsByBucket.get("bucket-b"));
        } finally {
            Files.deleteIfExists(fileA);
            Files.deleteIfExists(fileB);
        }
    }

    @Test
    public void testUploadFilesProgressListenerReceivesProgress() throws Exception {
        Path file = Files.createTempFile("volume-upload-progress", ".txt");
        Files.write(file, Collections.singletonList("data"));
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));
        List<UploadProgress> progressEvents = new CopyOnWriteArrayList<>();

        try {
            manager.uploadFiles(UploadFilesRequest.builder()
                    .sourceFilePath(file.toString())
                    .targetVolumePath("progress/")
                    .uploadConcurrency(1)
                    .progressListener(progressEvents::add)
                    .build());

            assertEquals(3, progressEvents.size());
            assertEquals(100.0, progressEvents.get(0).getPercent());
            assertEquals(file.toString(), progressEvents.get(0).getCurrentFile());
            assertEquals("", progressEvents.get(progressEvents.size() - 1).getCurrentFile());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testUploadFilesProgressListenerFailureDoesNotRetry() throws Exception {
        Path file = Files.createTempFile("volume-upload-progress-failure", ".txt");
        Files.write(file, Collections.singletonList("data"));
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));

        try {
            UploadFilesRequest request = UploadFilesRequest.builder()
                    .sourceFilePath(file.toString())
                    .targetVolumePath("failure/")
                    .uploadConcurrency(1)
                    .maxRetries(3)
                    .progressListener(progress -> {
                        throw new RuntimeException("stop");
                    })
                    .build();

            assertThrows(Exception.class, () -> manager.uploadFiles(request));
            assertEquals(1, manager.uploadsByBucket.get("bucket-failure").size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testUploadFilesClosesRequestStorageClient() throws Exception {
        Path file = Files.createTempFile("volume-upload-close", ".txt");
        Files.write(file, Collections.singletonList("data"));
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));

        try {
            manager.uploadFiles(UploadFilesRequest.builder()
                    .sourceFilePath(file.toString())
                    .targetVolumePath("close/")
                    .uploadConcurrency(1)
                    .build());

            assertEquals(Collections.singletonList("bucket-close"), manager.closedBuckets);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testUploadFilesMaxRetriesZeroDisablesRetry() throws Exception {
        Path file = Files.createTempFile("volume-upload-no-retry", ".txt");
        Files.write(file, Collections.singletonList("data"));
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));
        manager.uploadException = new IOException("temporary");

        try {
            UploadFilesRequest request = UploadFilesRequest.builder()
                    .sourceFilePath(file.toString())
                    .targetVolumePath("no-retry/")
                    .uploadConcurrency(1)
                    .maxRetries(0)
                    .retryIntervalMillis(0L)
                    .build();

            assertThrows(Exception.class, () -> manager.uploadFiles(request));
            assertEquals(1, manager.attemptsByBucket.get("bucket-no-retry").get());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testUploadFilesDoesNotRetryNonRetryableFailure() throws Exception {
        Path file = Files.createTempFile("volume-upload-param-failure", ".txt");
        Files.write(file, Collections.singletonList("data"));
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));
        manager.uploadException = new ParamException("invalid part size");

        try {
            UploadFilesRequest request = UploadFilesRequest.builder()
                    .sourceFilePath(file.toString())
                    .targetVolumePath("param-failure/")
                    .uploadConcurrency(1)
                    .maxRetries(3)
                    .retryIntervalMillis(0L)
                    .build();

            assertThrows(Exception.class, () -> manager.uploadFiles(request));
            assertEquals(1, manager.attemptsByBucket.get("bucket-param-failure").get());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testUploadFilesProgressUsesCanonicalPathForRelativeSingleFile() throws Exception {
        Path dir = Paths.get("target", "volume-relative-progress");
        Files.createDirectories(dir);
        Path file = dir.resolve("relative-progress.txt");
        Files.write(file, Collections.singletonList("data"));
        String relativeFilePath = Paths.get("").toAbsolutePath().relativize(file.toAbsolutePath()).toString();
        ConcurrentVolumeFileManager manager = new ConcurrentVolumeFileManager(new CyclicBarrier(1));
        List<UploadProgress> progressEvents = new CopyOnWriteArrayList<>();

        try {
            manager.uploadFiles(UploadFilesRequest.builder()
                    .sourceFilePath(relativeFilePath)
                    .targetVolumePath("relative-progress/")
                    .uploadConcurrency(1)
                    .progressListener(progressEvents::add)
                    .build());

            assertFalse(progressEvents.isEmpty());
            long fileSize = file.toFile().length();
            assertTrue(progressEvents.stream().allMatch(progress -> progress.getUploadedBytes() <= fileSize));
            assertEquals(fileSize, progressEvents.get(progressEvents.size() - 1).getUploadedBytes());
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(dir);
        }
    }

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

    private static class ConcurrentVolumeFileManager extends VolumeFileManager {
        private final Gson gson = new Gson();
        private final CyclicBarrier barrier;
        private final Map<String, List<String>> uploadsByBucket = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> attemptsByBucket = new ConcurrentHashMap<>();
        private final List<String> closedBuckets = new CopyOnWriteArrayList<>();
        private volatile Exception uploadException;

        private ConcurrentVolumeFileManager(CyclicBarrier barrier) throws Exception {
            super(VolumeFileManagerParam.newBuilder()
                    .withCloudEndpoint("https://api.cloud.zilliz.com")
                    .withApiKey("test-api-key")
                    .withVolumeName("test-volume")
                    .build());
            this.barrier = barrier;
        }

        @Override
        protected String applyVolume(ApplyVolumeRequest applyVolumeRequest) {
            String name = applyVolumeRequest.getPath().replace("/", "");
            ApplyVolumeResponse response = ApplyVolumeResponse.builder()
                    .endpoint("s3.amazonaws.com")
                    .cloud("aws")
                    .region("us-west-2")
                    .bucketName("bucket-" + name)
                    .uploadPath("")
                    .credentials(ApplyVolumeResponse.Credentials.builder()
                            .tmpAK("ak")
                            .tmpSK("sk")
                            .sessionToken("token")
                            .expireTime("2099-12-31T23:59:59Z")
                            .build())
                    .condition(ApplyVolumeResponse.Condition.builder()
                            .maxContentLength(1024L * 1024L)
                            .build())
                    .volumeName("test-volume")
                    .volumePrefix("prefix-" + name + "/")
                    .build();
            return gson.toJson(response);
        }

        @Override
        protected StorageClient createStorageClient(ApplyVolumeResponse applyVolumeResponse) {
            uploadsByBucket.putIfAbsent(applyVolumeResponse.getBucketName(), new CopyOnWriteArrayList<>());
            attemptsByBucket.putIfAbsent(applyVolumeResponse.getBucketName(), new AtomicInteger(0));
            return new RecordingStorageClient(
                    barrier, applyVolumeResponse.getBucketName(), uploadsByBucket, attemptsByBucket,
                    closedBuckets, () -> uploadException);
        }
    }

    @FunctionalInterface
    private interface UploadExceptionSupplier {
        Exception get();
    }

    private static class RecordingStorageClient implements StorageClient {
        private final CyclicBarrier barrier;
        private final String bucketName;
        private final Map<String, List<String>> uploadsByBucket;
        private final Map<String, AtomicInteger> attemptsByBucket;
        private final List<String> closedBuckets;
        private final UploadExceptionSupplier uploadExceptionSupplier;

        private RecordingStorageClient(CyclicBarrier barrier, String bucketName,
                                       Map<String, List<String>> uploadsByBucket,
                                       Map<String, AtomicInteger> attemptsByBucket,
                                       List<String> closedBuckets,
                                       UploadExceptionSupplier uploadExceptionSupplier) {
            this.barrier = barrier;
            this.bucketName = bucketName;
            this.uploadsByBucket = uploadsByBucket;
            this.attemptsByBucket = attemptsByBucket;
            this.closedBuckets = closedBuckets;
            this.uploadExceptionSupplier = uploadExceptionSupplier;
        }

        @Override
        public Long getObjectEntity(String bucketName, String objectKey) {
            return 0L;
        }

        @Override
        public boolean checkBucketExist(String bucketName) {
            return true;
        }

        @Override
        public void putObject(File file, String bucketName, String objectKey) throws Exception {
            putObject(file, bucketName, objectKey, null);
        }

        @Override
        public void putObject(File file, String bucketName, String objectKey,
                              UploadProgressListener progressListener) throws Exception {
            barrier.await();
            attemptsByBucket.get(bucketName).incrementAndGet();
            Exception uploadException = uploadExceptionSupplier.get();
            if (uploadException != null) {
                throw uploadException;
            }
            uploadsByBucket.get(bucketName).add(objectKey);
            if (progressListener != null) {
                progressListener.onProgress(file.length());
            }
        }

        @Override
        public void close() {
            closedBuckets.add(bucketName);
        }
    }
}
