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

import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import io.milvus.bulkwriter.model.UploadProgress;
import io.milvus.bulkwriter.request.volume.UploadFilesRequest;
import io.milvus.bulkwriter.response.ApplyVolumeResponse;
import io.milvus.bulkwriter.storage.StorageClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Explicit opt-in only. Writes test objects under a unique prefix; never deletes the Volume. */
@Tag("integration")
class VolumeFileManagerLiveIT {
    private static final long OLD_MTIME = 1577836800000L;
    @TempDir Path local;

    @Test
    void realPoliciesPaginationAndAdaptiveIndex() throws Exception {
        assumeTrue("true".equals(System.getenv("MILVUS_VOLUME_LIVE_TEST")), "Real cloud test is opt-in");
        assumeTrue(System.getenv("MILVUS_VOLUME_EXISTING_PREFIX") == null, "Existing-prefix verification mode");
        VolumeFileManagerParam params = parameters();
        String prefix = "sdk-volume-live-test/" + UUID.randomUUID() + "/";
        System.out.println("LIVE_TEST_PREFIX=" + prefix);
        Path scratch = Files.createDirectory(local.resolve("scratch"));
        RecordingManager manager = new RecordingManager(params, scratch);
        try {
            Path small = Files.createDirectory(local.resolve("small"));
            Path changedSize = write(small, "a/same.txt", 3, (byte) 1);
            Path changedTime = write(small, "b/same.txt", 5, (byte) 2);
            write(small, "a/nested/中文 + %.txt", 7, (byte) 3);
            write(small, "empty.txt", 0, (byte) 0);
            run(manager, small, prefix + "small/", UploadPolicy.OVERWRITE, 4, 0, false, "initial overwrite");
            run(manager, small, prefix + "small/", null, 0, 1, false, "default same size repeat");
            write(small, "a/same.txt", 4, (byte) 4);
            run(manager, small, prefix + "small/", UploadPolicy.SKIP_IF_EXISTS, 0, 1, false, "exists ignores size change");
            run(manager, small, prefix + "small/", null, 1, 1, false, "default uploads changed size");
            write(small, "b/same.txt", 5, (byte) 9);
            run(manager, small, prefix + "small/", null, 0, 1, false, "same size ignores changed content");
            run(manager, small, prefix + "small/", UploadPolicy.OVERWRITE, 4, 0, false, "forced overwrite");
            run(manager, small, prefix + "small/", UploadPolicy.SIZE_AND_MTIME, 0, 1, false, "older local mtime");
            assertTrue(manager.maxRemoteTime.get() > OLD_MTIME, "LIST must return server LastModified");
            Files.setLastModifiedTime(changedTime, FileTime.fromMillis(manager.maxRemoteTime.get() + 86400000L));
            run(manager, small, prefix + "small/", UploadPolicy.SIZE_AND_MTIME, 1, 1, false, "newer local mtime");
            run(manager, changedSize, prefix + "small/a/", null, 0, 1, false, "single file exact key");

            // 100 files fit in each of 100 nested directories, with one extra file to trigger spill.
            Path large = Files.createDirectory(local.resolve("large"));
            int count = UploadManifest.MEMORY_FILE_LIMIT + 1;
            for (int i = 0; i < count; i++) {
                write(large, "folder-" + (i % 10) + "/nested-" + ((i / 10) % 10) + "/file-" + i + ".txt", 16, (byte) i);
            }
            run(manager, large, prefix + "large/", UploadPolicy.OVERWRITE, count, 0, true, "large initial upload");
            run(manager, large, prefix + "large/", null, 0, (count + 999) / 1000, true, "large paginated repeat");
            assertEquals(count, manager.listedObjects.get(), "Every uploaded object must be found through LIST");
            System.out.println("LIVE_TEST_PASS prefix=" + prefix + " remoteObjects=" + (count + 4));
        } finally {
            manager.shutdownGracefully();
            System.out.println("LIVE_TEST_REMOTE_FILES_RETAINED prefix=" + prefix);
        }
    }

    /** Reuses this test's existing objects to check more local sizes without another bulk upload. */
    @Test
    void existingRemotePrefixAtIndexThresholds() throws Exception {
        assumeTrue("true".equals(System.getenv("MILVUS_VOLUME_LIVE_TEST")), "Real cloud test is opt-in");
        String prefix = System.getenv("MILVUS_VOLUME_EXISTING_PREFIX");
        assumeTrue(prefix != null, "No existing test prefix selected");
        assertTrue(prefix.matches("sdk-volume-live-test/[a-f0-9-]{36}/"), "Only isolated live-test prefixes are supported");
        Path scratch = Files.createDirectory(local.resolve("scratch"));
        Path source = Files.createDirectory(local.resolve("source"));
        RecordingManager manager = new RecordingManager(parameters(), scratch);
        int written = 0;
        try {
            for (int count : new int[]{100, 1000, 1001, 10000, 10001}) {
                while (written < count) {
                    int i = written++;
                    write(source, "folder-" + (i % 10) + "/nested-" + ((i / 10) % 10) + "/file-" + i + ".txt", 16, (byte) i);
                }
                run(manager, source, prefix + "large/", null, 0, -1,
                        count > UploadManifest.MEMORY_FILE_LIMIT, "existing prefix localFiles=" + count);
            }
            System.out.println("LIVE_TEST_BOUNDARIES_PASS prefix=" + prefix);
        } finally { manager.shutdownGracefully(); }
    }

    private static VolumeFileManagerParam parameters() throws IOException {
        Properties config = new Properties();
        String configPath = System.getenv("MILVUS_VOLUME_TEST_CONFIG");
        if (configPath != null) {
            try (Reader reader = Files.newBufferedReader(Paths.get(configPath), StandardCharsets.UTF_8)) {
                config.load(reader);
            }
        }
        return VolumeFileManagerParam.newBuilder()
                .withCloudEndpoint(required(config, "endpoint", "MILVUS_VOLUME_TEST_ENDPOINT"))
                .withApiKey(required(config, "apiKey", "MILVUS_VOLUME_TEST_API_KEY"))
                .withVolumeName(required(config, "managerVolumeName", "MILVUS_VOLUME_TEST_NAME")).build();
    }

    private static String required(Properties config, String key, String env) {
        String value = System.getenv(env);
        if (value == null || value.trim().isEmpty()) { value = config.getProperty(key); }
        if (value == null || value.trim().isEmpty()) { throw new IllegalArgumentException("Missing test setting: " + key); }
        return value.trim();
    }

    private static Path write(Path root, String relative, int size, byte value) throws IOException {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        byte[] content = new byte[size];
        Arrays.fill(content, value);
        Files.write(file, content);
        Files.setLastModifiedTime(file, FileTime.fromMillis(OLD_MTIME));
        return file;
    }

    private static void run(RecordingManager manager, Path source, String target, UploadPolicy policy,
                            int expectedPuts, int expectedLists, boolean disk, String scenario) throws Exception {
        manager.reset();
        AtomicReference<UploadProgress> progress = new AtomicReference<>();
        UploadFilesRequest request = UploadFilesRequest.builder().sourceFilePath(source.toString())
                .targetVolumePath(target).temporaryDirectory(manager.scratch.toString())
                .uploadConcurrency(5).maxRetries(0).progressListener(value -> {
                    progress.set(value);
                    System.out.println("LIVE_TEST_PROGRESS=" + scenario + " completed=" + value.getCompletedFiles()
                            + " total=" + value.getTotalFiles() + " bytes=" + value.getUploadedBytes());
                }).build();
        if (policy != null) { request.setUploadPolicy(policy); }
        System.out.println("LIVE_TEST_START=" + scenario);
        long started = System.nanoTime();
        manager.uploadFiles(request);
        assertEquals(expectedPuts, manager.puts.get(), scenario + ": successful PUT count");
        if (expectedLists >= 0) { assertEquals(expectedLists, manager.lists.get(), scenario + ": LIST count"); }
        else { assertTrue(manager.lists.get() > 0, scenario + ": LIST must check existing files"); }
        assertEquals(disk, manager.diskIndexSeen, scenario + ": index storage");
        assertNotNull(progress.get(), scenario + ": final progress");
        assertEquals(expectedPuts, progress.get().getCompletedFiles(), scenario + ": completed files");
        assertEquals(100.0, progress.get().getPercent(), scenario + ": completion percentage");
        try (Stream<Path> files = Files.list(manager.scratch)) {
            assertEquals(0L, files.count(), scenario + ": temporary index cleanup");
        }
        System.out.println("LIVE_TEST_SCENARIO=" + scenario + " puts=" + manager.puts.get()
                + " lists=" + manager.lists.get() + " listedObjects=" + manager.listedObjects.get()
                + " index=" + (disk ? "sqlite" : "memory")
                + " elapsedMillis=" + (System.nanoTime() - started) / 1000000);
    }

    private static final class RecordingManager extends VolumeFileManager {
        final Path scratch;
        final AtomicInteger puts = new AtomicInteger();
        final AtomicInteger lists = new AtomicInteger();
        final AtomicLong listedObjects = new AtomicLong();
        final AtomicLong maxRemoteTime = new AtomicLong();
        boolean diskIndexSeen;

        RecordingManager(VolumeFileManagerParam params, Path scratch) {
            super(params);
            this.scratch = scratch;
        }

        void reset() {
            puts.set(0);
            lists.set(0);
            listedObjects.set(0);
            maxRemoteTime.set(0);
            diskIndexSeen = false;
        }

        @Override
        protected StorageClient createStorageClient(ApplyVolumeResponse response) {
            try (Stream<Path> files = Files.list(scratch)) {
                diskIndexSeen = files.anyMatch(path -> Files.exists(path.resolve("manifest.db")));
            } catch (IOException e) { throw new UncheckedIOException(e); }
            StorageClient delegate = super.createStorageClient(response);
            return new StorageClient() {
                @Override public Long getObjectEntity(String bucket, String key) {
                    throw new AssertionError("Live test must not use HEAD/GET object access");
                }
                @Override public boolean checkBucketExist(String bucket) {
                    throw new AssertionError("Live test must not use bucket-wide access");
                }
                @Override public ObjectListPage listObjectsPage(String bucket, String prefix, String token) throws Exception {
                    lists.incrementAndGet();
                    ObjectListPage page = delegate.listObjectsPage(bucket, prefix, token);
                    listedObjects.addAndGet(page.getObjects().size());
                    for (ObjectMetadata object : page.getObjects()) {
                        assertNotNull(object.getSize(), "Live LIST must return Size");
                        assertNotNull(object.getLastModifiedTimeMillis(), "Live LIST must return LastModified");
                        maxRemoteTime.accumulateAndGet(object.getLastModifiedTimeMillis(), Math::max);
                    }
                    return page;
                }
                @Override public void putObject(File file, String bucket, String key) throws Exception {
                    putObject(file, bucket, key, null, 0);
                }
                @Override public void putObject(File file, String bucket, String key,
                        UploadProgressListener listener, long partSize) throws Exception {
                    delegate.putObject(file, bucket, key, listener, partSize);
                    puts.incrementAndGet();
                }
                @Override public void close() { delegate.close(); }
            };
        }
    }
}
