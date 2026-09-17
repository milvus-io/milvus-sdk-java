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
import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import java.time.Instant;
import io.milvus.bulkwriter.model.UploadProgress;
import io.milvus.bulkwriter.request.volume.ApplyVolumeRequest;
import io.milvus.bulkwriter.request.volume.UploadFilesRequest;
import io.milvus.bulkwriter.response.ApplyVolumeResponse;
import io.milvus.bulkwriter.storage.StorageClient;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class VolumeFileManagerUploadPolicyTest {
    @TempDir
    Path directory;

    @Test
    void overwriteDoesNotListObjects() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/data.txt", 5L);
        UploadFilesRequest request = request(file);
        request.setUploadPolicy(UploadPolicy.OVERWRITE);
        manager.uploadFiles(request);
        assertEquals(0, manager.listCalls.get());
        assertEquals(1, manager.puts.get());
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/data.txt"));
        assertEquals(UploadPolicy.SKIP_IF_SAME_SIZE, new UploadFilesRequest().getUploadPolicy());
        assertEquals(UploadPolicy.SKIP_IF_SAME_SIZE, new UploadFilesRequest("source", "target").getUploadPolicy());
        assertEquals(UploadPolicy.SKIP_IF_SAME_SIZE, UploadFilesRequest.builder().build().getUploadPolicy());
    }

    @Test
    void existingFileWithDifferentSizeIsSkippedEvenWhenQuotaIsExhausted() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/data.txt", 0L);
        manager.maxBytes = 0;
        manager.maxFiles = 0L;
        List<UploadProgress> events = new CopyOnWriteArrayList<>();
        UploadFilesRequest request = request(file);
        request.setProgressListener(events::add);
        assertEquals("data/", manager.uploadFiles(request).getPath());
        assertEquals(0, manager.puts.get());
        UploadProgress last = events.get(events.size() - 1);
        assertEquals(100.0, last.getPercent());
        assertEquals(0L, last.getTotalBytes());
        assertEquals(0L, last.getTotalFiles());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void nestedFilesUseFullKeyAndOnlyMissingFilesConsumeQuotaAndProgress() throws Exception {
        write("a/same.txt", 10);
        write("b/same.txt", 3);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/a/same.txt", 10L);
        manager.maxBytes = 3;
        manager.maxFiles = 1L;
        List<UploadProgress> events = new CopyOnWriteArrayList<>();
        UploadFilesRequest request = request(directory);
        request.setTargetVolumePath("/data/./");
        request.setProgressListener(events::add);
        manager.uploadFiles(request);
        assertEquals(1, manager.puts.get());
        assertEquals(1, manager.listCalls.get());
        assertEquals(Long.valueOf(3), manager.objects.get("prefix/data/b/same.txt"));
        UploadProgress last = events.get(events.size() - 1);
        assertEquals(3L, last.getTotalBytes());
        assertEquals(3L, last.getUploadedBytes());
        assertEquals(1, last.getCompletedFiles());
        assertEquals(100.0, last.getPercent());
    }

    @Test
    void allChecksFinishBeforeQuotaFailureAndNoObjectsAreWritten() throws Exception {
        write("a.txt", 3);
        write("b.txt", 3);
        FakeManager manager = new FakeManager();
        manager.maxBytes = 5;
        assertThrows(ExecutionException.class, () -> manager.uploadFiles(request(directory)));
        assertEquals(1, manager.listCalls.get());
        assertEquals(0, manager.puts.get());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void listingPermissionAndBucketErrorsDoNotFallThroughToUpload() throws Exception {
        Path file = write("data.txt", 5);
        for (String code : new String[]{"AccessDenied", "NoSuchBucket", "InvalidAccessKeyId",
                "SignatureDoesNotMatch", "InvalidToken", "InvalidSecurityToken"}) {
            FakeManager manager = new FakeManager();
            manager.listFailure = s3Error(code);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> manager.uploadFiles(request(file)));
            assertSame(manager.listFailure, failure.getCause().getCause().getCause());
            assertEquals(1, manager.listCalls.get());
            assertEquals(0, manager.puts.get());
            assertEquals(1, manager.applies.get());
            assertEquals(manager.applies.get(), manager.closes.get());
        }
    }

    @Test
    void expiredListingCredentialsRefreshAndUseANewSession() throws Exception {
        Path file = write("data.txt", 5);
        for (String code : new String[]{"ExpiredToken", "SecurityTokenExpired"}) {
            for (int status : new int[]{400, 403}) {
                FakeManager manager = new FakeManager();
                manager.firstSessionListFailure = s3Error(code, status);
                manager.objects.put("prefix/data/data.txt", 5L);
                manager.uploadFiles(request(file));
                assertEquals(Arrays.asList(1, 2), manager.listSessions);
                assertEquals(2, manager.listCalls.get());
                assertEquals(2, manager.applies.get());
                assertEquals(0, manager.puts.get());
                assertEquals(manager.applies.get(), manager.closes.get());
            }
        }
    }

    @Test
    void expiredCredentialsOnLaterPagePreserveCursorAndEarlierMatches() throws Exception {
        write("a.txt", 0);
        write("z.txt", 0);
        for (String code : new String[]{"ExpiredToken", "SecurityTokenExpired"}) {
            FakeManager manager = new FakeManager();
            manager.objects.put("prefix/data/a.txt", 0L);
            manager.objects.put("prefix/data/z.txt", 0L);
            for (int i = 0; i < 1000; i++) { manager.objects.put("prefix/data/m" + i, 0L); }
            manager.firstSessionListFailure = s3Error(code);
            manager.failExpiredListOnNextPageOnly = true;
            manager.uploadFiles(request(directory));
            assertEquals(Arrays.asList(null, "1000", "1000"), manager.pageTokens);
            assertEquals(Arrays.asList(1, 1, 2), manager.listSessions);
            assertEquals(2, manager.applies.get());
            assertEquals(0, manager.puts.get());
            assertEquals(manager.applies.get(), manager.closes.get());
        }
    }

    @Test
    void expiredPutCredentialsRefreshAndRetryUnderEveryPolicy() throws Exception {
        Path file = write("data.txt", 5);
        for (String code : new String[]{"ExpiredToken", "SecurityTokenExpired"}) {
            for (int status : new int[]{400, 403}) {
                for (UploadPolicy policy : UploadPolicy.values()) {
                    FakeManager manager = new FakeManager();
                    manager.firstSessionPutFailure = s3Error(code, status);
                    UploadFilesRequest request = request(file);
                    request.setUploadPolicy(policy);
                    manager.uploadFiles(request);
                    assertEquals(Arrays.asList(1, 2), manager.putSessions);
                    assertEquals(2, manager.puts.get());
                    assertEquals(2, manager.applies.get());
                    assertEquals(policy == UploadPolicy.OVERWRITE ? 0 : 1, manager.perObjectChecks.get());
                    assertEquals(policy == UploadPolicy.OVERWRITE ? 0 : 2, manager.listCalls.get());
                    assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/data.txt"));
                    assertEquals(manager.applies.get(), manager.closes.get());
                }
            }
        }
    }

    @Test
    void repeatedCredentialExpirationHonorsRetryLimitIncludingZero() throws Exception {
        Path file = write("data.txt", 5);
        for (String code : new String[]{"ExpiredToken", "SecurityTokenExpired"}) {
            for (boolean listing : new boolean[]{true, false}) {
                for (int retries : new int[]{0, 2}) {
                    FakeManager manager = new FakeManager();
                    UploadFilesRequest request = request(file);
                    request.setMaxRetries(retries);
                    if (listing) { manager.listFailure = s3Error(code); }
                    else {
                        manager.putFailure = s3Error(code);
                        request.setUploadPolicy(UploadPolicy.OVERWRITE);
                    }
                    assertThrows(ExecutionException.class, () -> manager.uploadFiles(request));
                    assertEquals(retries + 1, manager.applies.get());
                    assertEquals(listing ? retries + 1 : 0, manager.listCalls.get());
                    assertEquals(listing ? 0 : retries + 1, manager.puts.get());
                    assertTrue(manager.objects.isEmpty());
                    assertEquals(manager.applies.get(), manager.closes.get());
                }
            }
        }
    }

    @Test
    void otherPutAuthorizationErrorsDoNotRefreshOrRetry() throws Exception {
        Path file = write("data.txt", 5);
        for (String code : new String[]{"AccessDenied", "SignatureDoesNotMatch", "InvalidAccessKeyId",
                "InvalidToken", "InvalidSecurityToken"}) {
            FakeManager manager = new FakeManager();
            manager.putFailure = s3Error(code);
            UploadFilesRequest request = request(file);
            request.setUploadPolicy(UploadPolicy.OVERWRITE);
            assertThrows(ExecutionException.class, () -> manager.uploadFiles(request));
            assertEquals(1, manager.puts.get());
            assertEquals(1, manager.applies.get());
            assertEquals(0, manager.listCalls.get());
            assertEquals(manager.applies.get(), manager.closes.get());
        }
    }

    @Test
    void concurrentExpiredPutsShareOneCredentialRefresh() throws Exception {
        write("a.txt", 1);
        write("b.txt", 1);
        FakeManager manager = new FakeManager();
        manager.firstSessionPutFailure = s3Error("ExpiredToken", 400);
        manager.firstSessionPutBarrier = new java.util.concurrent.CyclicBarrier(2);
        UploadFilesRequest request = request(directory);
        request.setUploadConcurrency(2);
        request.setUploadPolicy(UploadPolicy.OVERWRITE);
        manager.uploadFiles(request);
        assertEquals(2, manager.applies.get());
        assertEquals(2, Collections.frequency(manager.putSessions, 1));
        assertEquals(2, Collections.frequency(manager.putSessions, 2));
        assertEquals(4, manager.puts.get());
        assertEquals(2, manager.objects.size());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void transientListingFailureRefreshesSessionAndRetries() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.failFirstList = true;
        manager.uploadFiles(request(file));
        assertEquals(2, manager.applies.get());
        assertEquals(1, manager.puts.get());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void listingRetryExhaustionDoesNotUpload() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.listFailure = new IOException("connection lost");
        assertThrows(ExecutionException.class, () -> manager.uploadFiles(request(file)));
        assertEquals(2, manager.listCalls.get());
        assertEquals(0, manager.puts.get());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void completedPutWithLostResponseIsNotUploadedAgain() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.losePutResponse = true;
        manager.uploadFiles(request(file));
        assertEquals(1, manager.puts.get());
        assertEquals(1, manager.perObjectChecks.get());
        assertEquals(2, manager.applies.get());
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/data.txt"));
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void repeatedDirectoryUploadSkipsEverythingOnSecondCall() throws Exception {
        write("a.txt", 3);
        write("nested/b.txt", 0);
        FakeManager manager = new FakeManager();
        manager.uploadFiles(request(directory));
        assertEquals(2, manager.puts.get());
        manager.maxBytes = 0;
        manager.uploadFiles(request(directory));
        assertEquals(2, manager.puts.get());
        assertEquals(2, manager.listCalls.get());
        assertEquals(0, manager.perObjectChecks.get());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void thousandAndFiveFilesShareTwoListingRequests() throws Exception {
        FakeManager manager = new FakeManager();
        for (int i = 0; i < 1005; i++) {
            String name = String.format("%04d.txt", i);
            write(name, 0);
            manager.objects.put("prefix/data/" + name, 0L);
        }
        manager.uploadFiles(request(directory));
        assertEquals(2, manager.listCalls.get());
        assertEquals(0, manager.perObjectChecks.get());
        assertEquals(0, manager.puts.get());
        assertEquals(Arrays.asList(null, "1000"), manager.pageTokens);
        assertEquals(Arrays.asList("prefix/data/", "prefix/data/"), manager.listingPrefixes);
    }

    @Test
    void stopsListingOnceAllLocalKeysHaveBeenFound() throws Exception {
        write("a.txt", 0);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/a.txt", 0L);
        for (int i = 0; i < 1001; i++) {
            manager.objects.put("prefix/data/z" + i + ".txt", 0L);
        }
        manager.uploadFiles(request(directory));
        assertEquals(1, manager.listCalls.get());
        assertEquals(0, manager.perObjectChecks.get());
        assertEquals(0, manager.puts.get());
    }

    @Test
    void nestedFilesShareListingOfRequestedTargetDirectory() throws Exception {
        write("2026/one/a.txt", 0);
        write("2026/two/b.txt", 0);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/2026/one/a.txt", 0L);
        manager.uploadFiles(request(directory));
        assertEquals(Collections.singletonList("prefix/data/"), manager.listingPrefixes);
        assertEquals(1, manager.listCalls.get());
        assertEquals(0, manager.perObjectChecks.get());
        assertEquals(1, manager.puts.get());
        assertTrue(manager.objects.containsKey("prefix/data/2026/two/b.txt"));
    }

    @Test
    void pageRetryPreservesCursorAndEarlierMatches() throws Exception {
        write("a.txt", 0);
        write("z.txt", 0);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/a.txt", 0L);
        manager.objects.put("prefix/data/z.txt", 0L);
        for (int i = 0; i < 1000; i++) {
            manager.objects.put("prefix/data/m" + i + ".txt", 0L);
        }
        manager.failNextPageOnce = true;
        manager.uploadFiles(request(directory));
        assertEquals(Arrays.asList(null, "1000", "1000"), manager.pageTokens);
        assertEquals(3, manager.listCalls.get());
        assertEquals(0, manager.perObjectChecks.get());
        assertEquals(0, manager.puts.get());
        assertEquals(2, manager.applies.get());
        assertEquals(manager.applies.get(), manager.closes.get());
    }

    @Test
    void emptyDirectoryNeedsNoListing() throws Exception {
        FakeManager manager = new FakeManager();
        manager.uploadFiles(request(directory));
        assertEquals(0, manager.listCalls.get());
        assertEquals(0, manager.puts.get());
    }

    @Test
    void asyncCancellationInterruptsWorkersAndCleansResources() throws Exception {
        Path file = write("source/data.txt", 5);
        Path scratch = Files.createDirectory(directory.resolve("scratch"));
        FakeManager manager = new FakeManager();
        manager.blockPut = true;
        UploadFilesRequest request = request(file);
        request.setTemporaryDirectory(scratch.toString());
        java.util.concurrent.CompletableFuture<?> upload = manager.uploadFilesAsync(request);
        assertTrue(manager.putStarted.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(upload.cancel(true));
        assertTrue(manager.putInterrupted.await(10, java.util.concurrent.TimeUnit.SECONDS));
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        boolean empty = false;
        while (System.nanoTime() < deadline) {
            try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(scratch)) {
                empty = !files.iterator().hasNext();
            }
            // Small uploads never create an index file, so an empty scratch directory
            // alone no longer proves that asynchronous resource cleanup has finished.
            if (empty && manager.applies.get() == manager.closes.get()) { break; }
            Thread.sleep(10);
        }
        assertTrue(empty, "Temporary index must be cleaned after cancellation");
        assertEquals(manager.applies.get(), manager.closes.get());
        assertEquals(1, manager.puts.get());
    }

    @Test
    void quotaFailureCleansTemporaryIndex() throws Exception {
        Path file = write("source/data.txt", 5);
        Path scratch = Files.createDirectory(directory.resolve("scratch"));
        FakeManager manager = new FakeManager();
        manager.maxBytes = 1;
        UploadFilesRequest request = request(file);
        request.setTemporaryDirectory(scratch.toString());
        assertThrows(ExecutionException.class, () -> manager.uploadFiles(request));
        try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(scratch)) {
            assertFalse(files.iterator().hasNext());
        }
    }

    @Test
    void defaultPolicySkipsOnlyMatchingSizesAndCountsRemainingQuota() throws Exception {
        write("same.txt", 5);
        write("changed.txt", 7);
        write("absent.txt", 2);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/same.txt", 5L);
        manager.objects.put("prefix/data/changed.txt", 6L);
        manager.objects.put("prefix/data/absent.txt.bak", 2L);
        manager.maxBytes = 9;
        manager.maxFiles = 2L;
        List<UploadProgress> events = new CopyOnWriteArrayList<>();
        UploadFilesRequest request = UploadFilesRequest.builder().sourceFilePath(directory.toString())
                .targetVolumePath("data/").progressListener(events::add).build();
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get());
        assertEquals(1, manager.listCalls.get());
        UploadProgress last = events.get(events.size() - 1);
        assertEquals(2, last.getCompletedFiles());
        assertEquals(9, last.getTotalBytes());
        assertEquals(9, last.getUploadedBytes());
    }

    @Test
    void sizeAndMtimeUploadsNewerOrDifferentSizedFiles() throws Exception {
        long time = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();
        for (String name : Arrays.asList("older", "equal", "newer", "different-size")) {
            Path file = write(name, 5);
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(time));
        }
        FakeManager manager = new FakeManager();
        for (String name : Arrays.asList("older", "equal", "newer", "different-size")) {
            manager.objects.put("prefix/data/" + name, name.equals("different-size") ? 6L : 5L);
        }
        manager.modifiedTimes.put("prefix/data/older", time + 1000);
        manager.modifiedTimes.put("prefix/data/equal", time);
        manager.modifiedTimes.put("prefix/data/newer", time - 1000);
        manager.modifiedTimes.put("prefix/data/different-size", time + 1000);
        UploadFilesRequest request = request(directory);
        request.setUploadPolicy(UploadPolicy.SIZE_AND_MTIME);
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get());
        assertEquals(Long.valueOf(time + 1000), manager.modifiedTimes.get("prefix/data/older"));
        assertEquals(Long.valueOf(time), manager.modifiedTimes.get("prefix/data/newer"));
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/different-size"));
    }

    @Test
    void missingRequiredMetadataDoesNotSkip() throws Exception {
        Path file = write("zero", 0);
        for (UploadPolicy policy : new UploadPolicy[]{UploadPolicy.SKIP_IF_SAME_SIZE, UploadPolicy.SIZE_AND_MTIME}) {
            FakeManager manager = new FakeManager();
            manager.objects.put("prefix/data/zero", 0L);
            manager.omitSize = policy == UploadPolicy.SKIP_IF_SAME_SIZE;
            manager.omitTime = policy == UploadPolicy.SIZE_AND_MTIME;
            UploadFilesRequest request = request(file);
            request.setUploadPolicy(policy);
            manager.uploadFiles(request);
            assertEquals(1, manager.puts.get());
        }
    }

    @Test
    void allKeysCheckedEndsListingEvenWhenSomeNeedOverwrite() throws Exception {
        write("a.txt", 5);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/a.txt", 4L);
        for (int i = 0; i < 1500; i++) { manager.objects.put("prefix/data/z" + i, 0L); }
        UploadFilesRequest request = request(directory);
        request.setUploadPolicy(UploadPolicy.SKIP_IF_SAME_SIZE);
        manager.uploadFiles(request);
        assertEquals(1, manager.listCalls.get());
        assertEquals(1, manager.puts.get());
    }

    @Test
    void committedPutWithLostResponseUsesSelectedPolicyOnRetry() throws Exception {
        Path file = write("data.txt", 5);
        for (UploadPolicy policy : UploadPolicy.values()) {
            FakeManager manager = new FakeManager();
            manager.losePutResponse = true;
            UploadFilesRequest request = request(file);
            request.setUploadPolicy(policy);
            manager.uploadFiles(request);
            assertEquals(policy == UploadPolicy.OVERWRITE ? 2 : 1, manager.puts.get(), policy.toString());
            assertEquals(policy == UploadPolicy.OVERWRITE ? 0 : 1, manager.perObjectChecks.get());
        }
    }

    @Test
    void retryDoesNotSkipAnExistingObjectWithWrongSize() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/data.txt", 4L);
        manager.failFirstPutBeforeCommit = true;
        UploadFilesRequest request = request(file);
        request.setUploadPolicy(UploadPolicy.SKIP_IF_SAME_SIZE);
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get());
        assertEquals(1, manager.perObjectChecks.get());
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/data.txt"));
    }

    @Test
    void retryDoesNotIgnoreTimeForSizeAndMtime() throws Exception {
        Path file = write("data.txt", 5);
        FakeManager manager = new FakeManager();
        manager.losePutResponse = true;
        manager.commitWithOlderTime = true;
        UploadFilesRequest request = request(file);
        request.setUploadPolicy(UploadPolicy.SIZE_AND_MTIME);
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get());
        assertEquals(1, manager.perObjectChecks.get());
    }

    @Test
    void defaultPolicySkipsZeroByteObjects() throws Exception {
        Path file = write("zero", 0);
        FakeManager manager = new FakeManager();
        manager.objects.put("prefix/data/zero", 0L);
        UploadFilesRequest request = new UploadFilesRequest(file.toString(), "data/");
        manager.uploadFiles(request);
        assertEquals(0, manager.puts.get());
        assertEquals(1, manager.listCalls.get());
    }

    @Test
    void nullPolicyIsRejectedAndExplicitPolicyIsRetained() {
        assertThrows(NullPointerException.class, () -> UploadFilesRequest.builder().uploadPolicy(null));
        assertThrows(NullPointerException.class, () -> new UploadFilesRequest().setUploadPolicy(null));
        for (UploadPolicy policy : UploadPolicy.values()) {
            UploadFilesRequest request = UploadFilesRequest.builder().uploadPolicy(policy).build();
            assertSame(policy, request.getUploadPolicy());
            assertTrue(request.toString().contains("uploadPolicy=" + policy));
        }
    }

    @Test
    void symbolicFileAndDirectorySourcesUploadAndSkipUsingLogicalKeys() throws Exception {
        Path file = write("external/data.txt", 5);
        Path source = Files.createDirectory(directory.resolve("source"));
        Path fileLink = Files.createSymbolicLink(source.resolve("alias.txt"), file);
        Files.createSymbolicLink(source.resolve("nested"), file.getParent());
        Path rootLink = Files.createSymbolicLink(directory.resolve("root-alias"), source);
        FakeManager manager = new FakeManager();
        UploadFilesRequest request = request(rootLink);
        request.setUploadPolicy(UploadPolicy.SKIP_IF_SAME_SIZE);
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get());
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/alias.txt"));
        assertEquals(Long.valueOf(5), manager.objects.get("prefix/data/nested/data.txt"));
        manager.uploadFiles(request);
        assertEquals(2, manager.puts.get(), "Both link paths should be skipped on a repeated upload");
        UploadFilesRequest single = request(fileLink);
        single.setUploadPolicy(UploadPolicy.OVERWRITE);
        manager.uploadFiles(single);
        assertEquals(3, manager.puts.get(), "A symlink can also be the single-file source");
    }

    @Test
    void blockingProgressCallbacksDoNotHoldTheTrackerMonitor() throws Exception {
        for (int operation = 0; operation < 3; operation++) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<VolumeFileManager.UploadProgressTracker> reference =
                    new AtomicReference<>();
            AtomicInteger callbacks = new AtomicInteger();
            VolumeFileManager.UploadProgressTracker tracker = new VolumeFileManager.UploadProgressTracker(20, 2, progress -> {
                assertFalse(Thread.holdsLock(reference.get()), "Listener must run outside the tracker monitor");
                if (callbacks.incrementAndGet() == 1) {
                    entered.countDown();
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });
            reference.set(tracker);
            ExecutorService workers = Executors.newFixedThreadPool(2);
            int trigger = operation;
            try {
                Future<?> blocked = workers.submit(() -> {
                    if (trigger == 0) { tracker.updateFile("first", 10, 1); }
                    else if (trigger == 1) { tracker.finishFile("first", 10); }
                    else { tracker.finishUpload(); }
                });
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                workers.submit(() -> {
                    tracker.resetFile("second");
                    // finishUpload does not consume the periodic emission interval.
                    if (trigger != 2) {
                        tracker.updateFile("second", 10, 10);
                        tracker.finishFile("second", 10);
                    }
                }).get(2, TimeUnit.SECONDS);
                release.countDown();
                blocked.get(3, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                workers.shutdownNow();
                assertTrue(workers.awaitTermination(3, TimeUnit.SECONDS));
            }
        }
    }

    private Path write(String name, int size) throws IOException {
        Path file = directory.resolve(name);
        Files.createDirectories(file.getParent());
        return Files.write(file, new byte[size]);
    }

    private UploadFilesRequest request(Path path) {
        return UploadFilesRequest.builder().sourceFilePath(path.toString()).targetVolumePath("data/")
                .uploadPolicy(UploadPolicy.SKIP_IF_EXISTS).maxRetries(1).retryIntervalMillis(0).build();
    }

    private static ErrorResponseException s3Error(String code) {
        return s3Error(code, code.startsWith("NoSuch") ? 404 : 403);
    }

    private static ErrorResponseException s3Error(String code, int status) {
        Response response = new Response.Builder()
                .request(new Request.Builder().url("https://example.com/bucket/key").get().build())
                .protocol(Protocol.HTTP_1_1).code(status)
                .message(code).build();
        return new ErrorResponseException(new ErrorResponse(code, code, "bucket", "key", "", "", ""),
                response, "");
    }

    static class FakeManager extends VolumeFileManager {
        final Map<String, Long> objects = new ConcurrentHashMap<>();
        final Map<String, Long> modifiedTimes = new ConcurrentHashMap<>();
        final AtomicInteger listCalls = new AtomicInteger();
        final AtomicInteger perObjectChecks = new AtomicInteger();
        final List<String> listingPrefixes = new CopyOnWriteArrayList<>();
        final List<String> pageTokens = new CopyOnWriteArrayList<>();
        final List<Integer> listSessions = new CopyOnWriteArrayList<>();
        final List<Integer> putSessions = new CopyOnWriteArrayList<>();
        final AtomicInteger puts = new AtomicInteger();
        final AtomicInteger applies = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        long maxBytes = 1024;
        Long maxFiles;
        Exception listFailure;
        Exception firstSessionListFailure;
        boolean failExpiredListOnNextPageOnly;
        Exception firstSessionPutFailure;
        Exception putFailure;
        java.util.concurrent.CyclicBarrier firstSessionPutBarrier;
        boolean failFirstList;
        boolean failNextPageOnce;
        boolean losePutResponse;
        boolean failFirstPutBeforeCommit;
        boolean commitWithOlderTime;
        boolean omitSize;
        boolean omitTime;
        boolean blockPut;
        final java.util.concurrent.CountDownLatch putStarted = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch putInterrupted = new java.util.concurrent.CountDownLatch(1);

        FakeManager() {
            super(VolumeFileManagerParam.newBuilder().withCloudEndpoint("https://example.com")
                    .withApiKey("test-key").withVolumeName("volume").build());
        }

        private StorageClient.ObjectMetadata metadata(String key) {
            return new StorageClient.ObjectMetadata(key, omitSize ? null : objects.get(key),
                    omitTime ? null : modifiedTimes.getOrDefault(key, 0L));
        }

        @Override
        protected String applyVolume(ApplyVolumeRequest request) {
            applies.incrementAndGet();
            Gson gson = new Gson();
            ApplyVolumeResponse.Condition condition = gson.fromJson(
                    "{\"maxContentLength\":" + maxBytes + ",\"maxFileNumber\":" + maxFiles + "}",
                    ApplyVolumeResponse.Condition.class);
            return gson.toJson(ApplyVolumeResponse.builder().volumeName("volume")
                    .volumePrefix("prefix/").bucketName("bucket")
                    .credentials(ApplyVolumeResponse.Credentials.builder()
                            .expireTime("2099-12-31T23:59:59Z").build())
                    .condition(condition).build());
        }

        @Override
        protected StorageClient createStorageClient(ApplyVolumeResponse response) {
            int session = applies.get();
            return new StorageClient() {
                @Override
                public Long getObjectEntity(String bucket, String key) {
                    throw new AssertionError("Volume upload must not use HEAD/GET object access");
                }

                @Override
                public ObjectMetadata findObject(String bucket, String key) {
                    perObjectChecks.incrementAndGet();
                    listCalls.incrementAndGet();
                    return objects.containsKey(key) ? metadata(key) : null;
                }

                @Override
                public ObjectListPage listObjectsPage(String bucket, String prefix, String token) throws Exception {
                    assertEquals("bucket", bucket);
                    listingPrefixes.add(prefix);
                    pageTokens.add(token);
                    listSessions.add(session);
                    int attempt = listCalls.incrementAndGet();
                    if (session == 1 && firstSessionListFailure != null
                            && (!failExpiredListOnNextPageOnly || token != null)) {
                        throw new ExecutionException(firstSessionListFailure);
                    }
                    if (listFailure != null) {
                        throw new ExecutionException(listFailure);
                    }
                    if (failFirstList && attempt == 1) {
                        throw new IOException("connection reset");
                    }
                    if (failNextPageOnce && token != null) {
                        failNextPageOnce = false;
                        throw new IOException("page response lost");
                    }
                    List<String> matching = objects.keySet().stream().filter(key -> key.startsWith(prefix))
                            .sorted().collect(Collectors.toList());
                    int start = token == null ? 0 : Integer.parseInt(token);
                    int end = Math.min(start + 1000, matching.size());
                    return new ObjectListPage(matching.subList(start, end).stream().map(FakeManager.this::metadata).collect(Collectors.toList()),
                            end < matching.size() ? String.valueOf(end) : null);
                }

                @Override
                public boolean checkBucketExist(String bucket) {
                    throw new AssertionError("Bucket-wide access is unnecessary");
                }

                @Override
                public void putObject(File file, String bucket, String key) throws Exception {
                    assertEquals("bucket", bucket);
                    int attempt = puts.incrementAndGet();
                    putSessions.add(session);
                    if (putFailure != null) { throw new ExecutionException(putFailure); }
                    if (session == 1 && firstSessionPutFailure != null) {
                        if (firstSessionPutBarrier != null) {
                            firstSessionPutBarrier.await(10, java.util.concurrent.TimeUnit.SECONDS);
                        }
                        throw new ExecutionException(firstSessionPutFailure);
                    }
                    if (failFirstPutBeforeCommit && attempt == 1) { throw new IOException("PUT failed before commit"); }
                    if (blockPut) {
                        putStarted.countDown();
                        try { new java.util.concurrent.CountDownLatch(1).await(); }
                        catch (InterruptedException e) { putInterrupted.countDown(); throw e; }
                    }
                    objects.put(key, file.length());
                    modifiedTimes.put(key, commitWithOlderTime ? file.lastModified() - 1000L : file.lastModified());
                    if (losePutResponse && attempt == 1) {
                        throw new IOException("response lost after commit");
                    }
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                }
            };
        }
    }
}
