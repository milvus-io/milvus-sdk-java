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
import io.milvus.bulkwriter.common.clientenum.ConnectType;
import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.model.UploadProgress;
import io.milvus.bulkwriter.request.volume.ApplyVolumeRequest;
import io.milvus.bulkwriter.request.volume.UploadFilesRequest;
import io.milvus.bulkwriter.resolver.EndpointResolver;
import io.milvus.bulkwriter.response.ApplyVolumeResponse;
import io.milvus.bulkwriter.restful.DataVolumeUtils;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.bulkwriter.storage.client.MinioStorageClient;
import io.milvus.exception.ParamException;
import io.minio.errors.ErrorResponseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class VolumeFileManager {
    private static final Logger logger = LoggerFactory.getLogger(VolumeFileManager.class);
    private static final long DEFAULT_CREDENTIAL_REFRESH_MARGIN_SECONDS = 300L;
    private final String cloudEndpoint;
    private final String apiKey;
    private final String volumeName;
    private final ConnectType connectType;

    private volatile VolumeSession lastVolumeSession;

    public VolumeFileManager(VolumeFileManagerParam volumeFileManagerParam) {
        this.cloudEndpoint = volumeFileManagerParam.getCloudEndpoint();
        this.apiKey = volumeFileManagerParam.getApiKey();
        this.volumeName = volumeFileManagerParam.getVolumeName();
        this.connectType = volumeFileManagerParam.getConnectType() == null ? ConnectType.AUTO : volumeFileManagerParam.getConnectType();
    }

    /**
     * Asynchronously uploads a local file or directory to the specified path within the Volume.
     *
     * @param request the upload request containing the source local file or directory path
     *                and the target directory path in the Volume {@link UploadFilesRequest}
     * @return a {@link CompletableFuture} that completes with an {@link UploadFilesResult}
     * once all files have been uploaded successfully. Scanning and preflight checks also run
     * asynchronously. A request-local adaptive index bounds heap use independently of file count;
     * uploads retain at most uploadConcurrency in-flight tasks. Cancellation interrupts the
     * workers and cleans up the index once in-flight operations have stopped.
     * @throws CompletionException if an error occurs during the upload process
     */
    public CompletableFuture<UploadFilesResult> uploadFilesAsync(UploadFilesRequest request) {
        Path source = Paths.get(request.getSourceFilePath()).toAbsolutePath().normalize();
        String volumePath = convertDirPath(request.getTargetVolumePath());
        int concurrency = Math.max(1, request.getUploadConcurrency());
        int maxRetries = Math.max(0, request.getMaxRetries());
        long retryInterval = Math.max(0L, request.getRetryIntervalMillis());
        long partSize = Math.max(0L, request.getPartSizeBytes());
        UploadPolicy uploadPolicy = java.util.Objects.requireNonNull(request.getUploadPolicy(), "uploadPolicy");
        UploadFilesRequest.ProgressListener listener = request.getProgressListener();
        Path temporaryDirectory = request.getTemporaryDirectory() == null ? null
                : Paths.get(request.getTemporaryDirectory()).toAbsolutePath();
        CompletableFuture<UploadFilesResult> result = new CompletableFuture<>();
        ExecutorService coordinator = Executors.newSingleThreadExecutor();
        Future<?> task = coordinator.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                result.complete(runUpload(source, volumePath, temporaryDirectory, concurrency, maxRetries,
                        retryInterval, partSize, uploadPolicy, listener));
            } catch (Throwable failure) {
                logUploadFailed(source.toString(), volumePath, startTime);
                result.completeExceptionally(failure);
            }
        });
        // Cancellation must interrupt the actual coordinator, not only the returned future.
        result.whenComplete((value, failure) -> {
            if (result.isCancelled()) { task.cancel(true); }
        });
        coordinator.shutdown();
        return result;
    }

    private UploadFilesResult runUpload(Path source, String volumePath, Path temporaryDirectory,
                                         int concurrency, int maxRetries, long retryInterval, long partSize,
                                         UploadPolicy uploadPolicy, UploadFilesRequest.ProgressListener listener) throws Exception {
        logger.info("Planning volume upload: sourcePath:{}, volumePath:{}, concurrency:{}, uploadPolicy:{}",
                source, volumePath, concurrency, uploadPolicy);
        try (UploadManifest manifest = new UploadManifest(temporaryDirectory)) {
            boolean singleFile = manifest.scan(source);
            UploadManifest.checkInterrupted();
            UploadContext context = new UploadContext(refreshVolumeAndClient(volumePath));
            try {
                String targetPrefix = context.currentSession().applyVolumeResponse.getVolumePrefix() + volumePath;
                if (uploadPolicy != UploadPolicy.OVERWRITE) {
                    filterExistingFiles(manifest, targetPrefix,
                            singleFile ? targetPrefix + source.getFileName() : targetPrefix,
                            volumePath, maxRetries, retryInterval, context, uploadPolicy);
                }
                long uploadBytes = manifest.remainingBytes();
                initValidator(manifest.remainingCount(), uploadBytes, context.currentSession().applyVolumeResponse);
                logger.info("Volume upload plan: filesToUpload:{}, skippedFiles:{}, bytesToUpload:{}",
                        manifest.remainingCount(), manifest.fileCount() - manifest.remainingCount(), uploadBytes);
                UploadProgressTracker tracker = new UploadProgressTracker(uploadBytes, manifest.remainingCount(), listener);
                Path base = singleFile ? source.getParent() : source;
                try (UploadManifest.Cursor cursor = manifest.openCursor()) {
                    BoundedUploadExecutor.run(concurrency, cursor::next, entry -> {
                        Path path = base.resolve(entry.relativePath);
                        validateSource(path, entry);
                        putObjectWithRetry(path.toFile(), entry.size, entry.modified, targetPrefix + entry.relativePath,
                                volumePath, maxRetries, retryInterval, tracker, context, partSize, uploadPolicy);
                        validateSource(path, entry);
                        tracker.finishFile(path.toString(), entry.size);
                    });
                }
                tracker.finishUpload();
                return UploadFilesResult.builder()
                        .volumeName(context.currentSession().applyVolumeResponse.getVolumeName())
                        .path(volumePath).build();
            } finally {
                context.closeSessions();
            }
        }
    }

    private void validateSource(Path path, UploadManifest.Entry entry) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        if (!attrs.isRegularFile() || attrs.size() != entry.size || attrs.lastModifiedTime().toMillis() != entry.modified) {
            throw new ParamException("Local file changed after upload planning: " + path);
        }
    }

    /**
     * Synchronously uploads a local file or directory to the specified path within the Volume.
     */
    public UploadFilesResult uploadFiles(UploadFilesRequest request) throws ExecutionException, InterruptedException {
        CompletableFuture<UploadFilesResult> result = uploadFilesAsync(request);
        try {
            return result.get();
        } catch (InterruptedException e) {
            result.cancel(true);
            throw e;
        }
    }

    public void shutdownGracefully() {
        VolumeSession session = lastVolumeSession;
        lastVolumeSession = null;
        closeVolumeSession(session);
    }

    private void logUploadFailed(String localDirOrFilePath, String volumePath, long startTime) {
        long endTime = System.currentTimeMillis();
        logger.warn("Volume upload failed: sourcePath:{}, volumeName:{}, volumePath:{}, endTime:{}, totalElapsed:{}",
                localDirOrFilePath, volumeName, volumePath,
                Instant.ofEpochMilli(endTime), formatDurationMillis(endTime - startTime));
    }

    private static String formatDurationMillis(long durationMillis) {
        if (durationMillis < 0L) {
            return "unknown";
        }
        long totalSeconds = (durationMillis + 999L) / 1000L;
        long hours = totalSeconds / 3600L;
        long minutes = (totalSeconds % 3600L) / 60L;
        long seconds = totalSeconds % 60L;
        if (hours > 0L) {
            return String.format(Locale.ROOT, "%dh %02dm %02ds", hours, minutes, seconds);
        }
        if (minutes > 0L) {
            return String.format(Locale.ROOT, "%dm %02ds", minutes, seconds);
        }
        return String.format(Locale.ROOT, "%ds", seconds);
    }

    private void initValidator(long uploadFileNumber, long uploadFileContentLength, ApplyVolumeResponse applyVolumeResponse) {
        Long maxContentLength = applyVolumeResponse.getCondition().getMaxContentLength();
        if (uploadFileContentLength > maxContentLength) {
            String msg = String.format("localFileTotalSize %s exceeds the maximum contentLength limit %s defined in the condition. If you are using the free tier, you may switch to the pay-as-you-go volume plan to support uploading larger files.",
                    uploadFileContentLength, maxContentLength);
            logger.error(msg);
            throw new ParamException(msg);
        }

        Long maxFileNumber = applyVolumeResponse.getCondition().getMaxFileNumber();
        if (maxFileNumber != null) {
            if (uploadFileNumber > maxFileNumber) {
                String msg = String.format(
                        "localFileTotalNumber %s exceeds the maximum fileNumber limit %s defined in the condition. If you are using the free tier, you may switch to the pay-as-you-go volume plan to support uploading more files.",
                        uploadFileNumber, maxFileNumber
                );
                logger.error(msg);
                throw new ParamException(msg);
            }
        }
    }

    private VolumeSession refreshVolumeAndClient(String path) {
        logger.info("refreshing Volume info...");
        ApplyVolumeRequest applyVolumeRequest = ApplyVolumeRequest.builder()
                .apiKey(apiKey)
                .volumeName(volumeName)
                .path(path)
                .build();
        String result = applyVolume(applyVolumeRequest);
        ApplyVolumeResponse applyVolumeResponse = new Gson().fromJson(result, ApplyVolumeResponse.class);
        logger.info("volume info refreshed");

        StorageClient storageClient = createStorageClient(applyVolumeResponse);
        VolumeSession session = new VolumeSession(applyVolumeResponse, storageClient,
                Instant.parse(applyVolumeResponse.getCredentials().getExpireTime()));
        lastVolumeSession = session;
        logger.info("storage client refreshed");
        return session;
    }

    protected String applyVolume(ApplyVolumeRequest applyVolumeRequest) {
        return DataVolumeUtils.applyVolume(cloudEndpoint, applyVolumeRequest);
    }

    protected StorageClient createStorageClient(ApplyVolumeResponse applyVolumeResponse) {
        String endpoint = EndpointResolver.resolveEndpoint(applyVolumeResponse.getEndpoint(), applyVolumeResponse.getCloud(),
                applyVolumeResponse.getRegion(), connectType);
        return MinioStorageClient.getStorageClient(
                applyVolumeResponse.getCloud(),
                endpoint,
                applyVolumeResponse.getCredentials().getTmpAK(),
                applyVolumeResponse.getCredentials().getTmpSK(),
                applyVolumeResponse.getCredentials().getSessionToken(),
                applyVolumeResponse.getRegion(), null);
    }

    private String convertDirPath(String inputPath) {
        if (StringUtils.isEmpty(inputPath) || inputPath.equals("/")) {
            return "";
        }
        String[] parts = inputPath.replace("\\", "/").split("/");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (StringUtils.isEmpty(part) || part.equals(".")) {
                continue;
            }
            if (part.equals("..")) {
                throw new ParamException("target volume path must not escape the volume root: " + inputPath);
            }
            if (builder.length() > 0) {
                builder.append("/");
            }
            builder.append(part);
        }
        if (builder.length() == 0) {
            return "";
        }
        return builder + "/";
    }

    private void filterExistingFiles(UploadManifest manifest, String targetPrefix, String listingPrefix,
                                     String volumePath, int maxRetries, long retryIntervalMillis,
                                     UploadContext uploadContext, UploadPolicy uploadPolicy) throws Exception {
        if (manifest.remainingCount() == 0) {
            logger.info("Volume existence check skipped: prefix:{}, reason:no local files", listingPrefix);
            return;
        }
        long listedPages = 0;
        long listedObjects = 0;
        long startedAt = System.nanoTime();
        long lastLogAt = startedAt;
        logger.info("Volume existence check started: prefix:{}, localFiles:{}", listingPrefix, manifest.fileCount());
        String continuationToken = null;
        do {
            UploadManifest.checkInterrupted();
            final String pageToken = continuationToken;
            StorageClient.ObjectListPage page = withRetry("list " + listingPrefix, () -> {
                refreshIfExpire(volumePath, uploadContext);
                try (SessionLease lease = uploadContext.acquire()) {
                    VolumeSession session = lease.session;
                    return session.storageClient.listObjectsPage(session.applyVolumeResponse.getBucketName(),
                            listingPrefix, pageToken);
                }
            }, volumePath, maxRetries, retryIntervalMillis, uploadContext);
            listedPages++;
            manifest.excludeExisting(page.getObjects(), targetPrefix, uploadPolicy);
            listedObjects += page.getObjects().size();
            long now = System.nanoTime();
            if (now - lastLogAt >= TimeUnit.SECONDS.toNanos(5)) {
                logger.info("Volume existence check progress: prefix:{}, listedPages:{}, listedObjects:{}, skippedFiles:{}, unmatchedLocalFiles:{}, elapsedMillis:{}",
                        listingPrefix, listedPages, listedObjects, manifest.fileCount() - manifest.remainingCount(),
                        manifest.unmatchedCount(), TimeUnit.NANOSECONDS.toMillis(now - startedAt));
                lastLogAt = now;
            }
            continuationToken = page.getNextContinuationToken();
        } while (continuationToken != null && manifest.unmatchedCount() > 0);
        logger.info("Volume existence check completed: prefix:{}, listedPages:{}, listedObjects:{}, skippedFiles:{}, filesToUpload:{}, elapsedMillis:{}, stopReason:{}",
                listingPrefix, listedPages, listedObjects, manifest.fileCount() - manifest.remainingCount(),
                manifest.remainingCount(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                manifest.unmatchedCount() == 0 ? "all local keys checked" : "end of listing");
    }

    private void putObjectWithRetry(File file, long fileSize, long modifiedTimeMillis, String remoteFilePath, String volumePath,
                                    int maxRetries, long retryIntervalMillis,
                                    UploadProgressTracker progressTracker,
                                    UploadContext uploadContext, long partSizeBytes, UploadPolicy uploadPolicy) {
        String msg = "upload " + file.getAbsolutePath();
        FileUploadProgress progress = new FileUploadProgress(progressTracker, file.getAbsolutePath(), fileSize);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);
        withRetry(msg, () -> {
            progress.reset();
            refreshIfExpire(volumePath, uploadContext);
            try (SessionLease lease = uploadContext.acquire()) {
                VolumeSession session = lease.session;
                // The first attempt was checked during planning. Recheck retries in case a PUT
                // succeeded remotely but its response was lost before reaching the client.
                if (uploadPolicy != UploadPolicy.OVERWRITE && !firstAttempt.getAndSet(false)) {
                    StorageClient.ObjectMetadata object = session.storageClient.findObject(
                            session.applyVolumeResponse.getBucketName(), remoteFilePath);
                    if (object != null && uploadPolicy.shouldSkip(fileSize, modifiedTimeMillis,
                            object.getSize(), object.getLastModifiedTimeMillis())) {
                        logger.debug("Skipping volume file on retry: key:{}, uploadPolicy:{}", remoteFilePath, uploadPolicy);
                        return null;
                    }
                }
                session.storageClient.putObject(file, session.applyVolumeResponse.getBucketName(), remoteFilePath, progress, partSizeBytes);
                return null;
            }
        }, volumePath, maxRetries, retryIntervalMillis, uploadContext);

    }

    private VolumeSession refreshIfExpire(String volumePath, UploadContext uploadContext) {
        VolumeSession session = uploadContext.currentSession();
        Instant refreshAt = session.expireTime.minusSeconds(DEFAULT_CREDENTIAL_REFRESH_MARGIN_SECONDS);
        if (Instant.now().isBefore(refreshAt)) {
            return session;
        }
        synchronized (uploadContext.refreshLock) {
            session = uploadContext.currentSession();
            refreshAt = session.expireTime.minusSeconds(DEFAULT_CREDENTIAL_REFRESH_MARGIN_SECONDS);
            if (Instant.now().isBefore(refreshAt)) {
                return session;
            }
            return refreshUploadContext(volumePath, uploadContext);
        }
    }

    private VolumeSession refreshUploadContext(String volumePath, UploadContext uploadContext) {
        VolumeSession session = refreshVolumeAndClient(volumePath);
        uploadContext.setSession(session);
        return session;
    }

    private static void closeVolumeSession(VolumeSession session) {
        if (session == null) {
            return;
        }
        if (session.closed.compareAndSet(false, true)) { session.storageClient.close(); }
    }

    private <T> T withRetry(String actionName, Callable<T> callable, String volumePath,
                            int maxRetries, long retryIntervalMillis,
                            UploadContext uploadContext) {
        int failedAttempts = 0;
        while (true) {
            VolumeSession attemptedSession = uploadContext.currentSession();
            try {
                UploadManifest.checkInterrupted();
                return callable.call();
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted() || hasCause(e, InterruptedException.class)) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(actionName + " interrupted", e);
                }
                if (hasCause(e, ProgressCallbackException.class)) {
                    throw new RuntimeException("Upload progress callback failed", e);
                }
                if (!isRetryableException(e)) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(actionName + " failed", e);
                }
                failedAttempts++;
                logger.warn("Attempt {} failed to {}", failedAttempts, actionName, e);
                if (failedAttempts > maxRetries) {
                    throw new RuntimeException(actionName + " failed after " + failedAttempts + " attempts", e);
                }
                synchronized (uploadContext.refreshLock) {
                    if (attemptedSession == uploadContext.currentSession()) {
                        refreshUploadContext(volumePath, uploadContext);
                    }
                }
                try {
                    Thread.sleep(retryIntervalMillis);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(actionName + " interrupted while waiting to retry", interruptedException);
                }
            }
        }
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> causeClass) {
        Throwable current = throwable;
        while (current != null) {
            if (causeClass.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isRetryableException(Throwable throwable) {
        if (hasCause(throwable, ParamException.class) || hasCause(throwable, IllegalArgumentException.class)) {
            return false;
        }
        ErrorResponseException errorResponseException = findCause(throwable, ErrorResponseException.class);
        if (errorResponseException != null) {
            return isRetryableS3Error(errorResponseException);
        }
        return hasCause(throwable, IOException.class) || hasCause(throwable, TimeoutException.class);
    }

    private boolean isRetryableS3Error(ErrorResponseException exception) {
        int statusCode = exception.response() == null ? 0 : exception.response().code();
        if (statusCode == 408 || statusCode == 429 || statusCode >= 500) {
            return true;
        }
        String code = exception.errorResponse() == null ? "" : exception.errorResponse().code();
        // Credential expiration is recoverable even when the service responds with 400/403.
        // withRetry refreshes the Volume session before retrying; other authorization errors
        // remain non-retryable. S3 uses ExpiredToken, OSS uses SecurityTokenExpired.
        return "ExpiredToken".equals(code)
                || "SecurityTokenExpired".equals(code)
                || "RequestTimeout".equals(code)
                || "SlowDown".equals(code)
                || "InternalError".equals(code)
                || "ServiceUnavailable".equals(code)
                || "Throttling".equals(code)
                || "ThrottlingException".equals(code)
                || "RequestLimitExceeded".equals(code);
    }

    private <T extends Throwable> T findCause(Throwable throwable, Class<T> causeClass) {
        Throwable current = throwable;
        while (current != null) {
            if (causeClass.isInstance(current)) {
                return causeClass.cast(current);
            }
            current = current.getCause();
        }
        return null;
    }

    private static class UploadContext {
        private final AtomicReference<VolumeSession> sessionRef;
        private final Object refreshLock = new Object();

        private UploadContext(VolumeSession session) {
            this.sessionRef = new AtomicReference<>(session);
        }

        private VolumeSession currentSession() { return sessionRef.get(); }

        private synchronized SessionLease acquire() {
            VolumeSession session = currentSession();
            session.users++;
            return new SessionLease(this, session);
        }

        private synchronized void setSession(VolumeSession session) {
            retire(sessionRef.getAndSet(session));
        }

        private synchronized void release(VolumeSession session) {
            session.users--;
            if (session.retired && session.users == 0) { closeVolumeSession(session); }
        }

        private void retire(VolumeSession session) {
            session.retired = true;
            if (session.users == 0) { closeVolumeSession(session); }
        }

        private synchronized void closeSessions() { retire(currentSession()); }
    }

    private static class SessionLease implements AutoCloseable {
        private final UploadContext context;
        private final VolumeSession session;

        private SessionLease(UploadContext context, VolumeSession session) {
            this.context = context;
            this.session = session;
        }

        @Override
        public void close() { context.release(session); }
    }

    static class UploadProgressTracker {
        private static final long LOG_INTERVAL_MILLIS = 5000L;

        private final long totalBytes;
        private final long totalFiles;
        private final UploadFilesRequest.ProgressListener progressListener;
        private final long startTimeMillis;
        private final Map<String, Long> fileProgress = new HashMap<>();
        private int completedFiles;
        private long uploadedBytes = 0L;
        private long lastLogTimeMillis = 0L;

        UploadProgressTracker(long totalBytes, long totalFiles,
                                      UploadFilesRequest.ProgressListener progressListener) {
            this.totalBytes = totalBytes;
            this.totalFiles = totalFiles;
            this.progressListener = progressListener;
            this.startTimeMillis = System.currentTimeMillis();
        }

        synchronized void resetFile(String filePath) {
            long previous = fileProgress.getOrDefault(filePath, 0L);
            uploadedBytes -= previous;
            fileProgress.put(filePath, 0L);
        }

        void updateFile(String filePath, long fileSize, long chunkBytes) {
            if (chunkBytes <= 0) { return; }
            UploadProgress progress;
            synchronized (this) {
                long previous = fileProgress.getOrDefault(filePath, 0L);
                long current = Math.min(fileSize, previous + chunkBytes);
                if (current <= previous) { return; }
                fileProgress.put(filePath, current);
                uploadedBytes += current - previous;
                progress = snapshotIfNeeded(filePath, current, fileSize);
            }
            if (progress != null) { emitProgress(progress); }
        }

        void finishFile(String filePath, long fileSize) {
            UploadProgress progress;
            synchronized (this) {
                Long previous = fileProgress.remove(filePath);
                uploadedBytes += fileSize - (previous == null ? 0L : previous);
                completedFiles++;
                progress = snapshotIfNeeded(filePath, fileSize, fileSize);
            }
            if (progress != null) { emitProgress(progress); }
        }

        void finishUpload() {
            UploadProgress progress;
            synchronized (this) {
                progress = snapshot("", 0L, 0L, 100.0);
            }
            emitProgress(progress);
        }

        // Called with the tracker monitor held; user callbacks must run after releasing it.
        private UploadProgress snapshotIfNeeded(String filePath, long uploaded, long size) {
            long now = System.currentTimeMillis();
            if (now - lastLogTimeMillis >= LOG_INTERVAL_MILLIS) {
                lastLogTimeMillis = now;
                return snapshot(filePath, uploaded, size, percent());
            }
            return null;
        }

        private double percent() {
            if (totalBytes == 0) {
                return totalFiles == 0 ? 100.0 : completedFiles * 100.0 / totalFiles;
            }
            return Math.min(100.0, uploadedBytes * 100.0 / totalBytes);
        }

        private long speedBps(long currentUploadedBytes, long nowMillis) {
            long elapsedMillis = Math.max(1L, nowMillis - startTimeMillis);
            return (long) (currentUploadedBytes * 1000.0 / elapsedMillis);
        }

        private long estimatedRemainingTimeMillis(long currentUploadedBytes, long speedBps) {
            long remainingBytes = Math.max(0L, totalBytes - currentUploadedBytes);
            if (remainingBytes == 0L) {
                return 0L;
            }
            if (speedBps <= 0L) {
                return -1L;
            }
            return (long) Math.ceil(remainingBytes * 1000.0 / speedBps);
        }

        private UploadProgress snapshot(String currentFile, long currentFileUploadedBytes,
                                        long currentFileTotalBytes, double percent) {
            return new UploadProgress(uploadedBytes, totalBytes, completedFiles, totalFiles,
                    currentFile, currentFileUploadedBytes, currentFileTotalBytes, percent);
        }

        private void emitProgress(UploadProgress progress) {
            long speedBps = speedBps(progress.getUploadedBytes(), System.currentTimeMillis());
            String estimatedRemainingTime = formatDurationMillis(estimatedRemainingTimeMillis(progress.getUploadedBytes(), speedBps));
            logger.info("Upload progress: {}/{} bytes, progress:{}%, files:{}/{}, speedBPS:{}, estimatedRemainingTime:{}",
                    progress.getUploadedBytes(), progress.getTotalBytes(),
                    String.format("%.2f", progress.getPercent()),
                    progress.getCompletedFiles(), progress.getTotalFiles(),
                    speedBps, estimatedRemainingTime);
            if (progressListener != null) {
                try {
                    progressListener.onProgress(progress);
                } catch (RuntimeException e) {
                    throw new ProgressCallbackException(e);
                }
            }
        }
    }

    private static class ProgressCallbackException extends RuntimeException {
        private ProgressCallbackException(Throwable cause) {
            super(cause);
        }
    }

    private static class FileUploadProgress implements StorageClient.UploadProgressListener {
        private final UploadProgressTracker progressTracker;
        private final String filePath;
        private final long fileSize;

        private FileUploadProgress(UploadProgressTracker progressTracker, String filePath, long fileSize) {
            this.progressTracker = progressTracker;
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        private void reset() {
            progressTracker.resetFile(filePath);
        }

        @Override
        public void onProgress(long bytes) {
            progressTracker.updateFile(filePath, fileSize, bytes);
        }
    }

    private static class VolumeSession {
        private final ApplyVolumeResponse applyVolumeResponse;
        private final StorageClient storageClient;
        private final Instant expireTime;
        private final AtomicBoolean closed = new AtomicBoolean();
        // Guarded by the owning UploadContext.
        private int users;
        private boolean retired;

        private VolumeSession(ApplyVolumeResponse applyVolumeResponse, StorageClient storageClient, Instant expireTime) {
            this.applyVolumeResponse = applyVolumeResponse;
            this.storageClient = storageClient;
            this.expireTime = expireTime;
        }
    }

}
