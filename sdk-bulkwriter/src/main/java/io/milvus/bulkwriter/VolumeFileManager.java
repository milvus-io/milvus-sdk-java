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
import io.milvus.bulkwriter.common.utils.FileUtils;
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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
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
     * once all files have been uploaded successfully
     * @throws CompletionException if an error occurs during the upload process
     */
    public CompletableFuture<UploadFilesResult> uploadFilesAsync(UploadFilesRequest request) {
        String localDirOrFilePath = request.getSourceFilePath();
        Pair<List<String>, Long> localPathPair = FileUtils.processLocalPath(localDirOrFilePath);
        String volumePath = convertDirPath(request.getTargetVolumePath());
        int uploadConcurrency = Math.max(1, request.getUploadConcurrency());
        int maxRetries = Math.max(0, request.getMaxRetries());
        long retryIntervalMillis = Math.max(0L, request.getRetryIntervalMillis());
        long partSizeBytes = Math.max(0L, request.getPartSizeBytes());
        long totalBytes = localPathPair.getValue();
        long totalFilesCount = localPathPair.getKey().size();
        long startTime = System.currentTimeMillis();
        logger.info("Starting volume upload: sourcePath:{}, volumeName:{}, volumePath:{}, totalFileCount:{}, totalFileSize:{} bytes ({}), uploadConcurrency:{}, maxRetries:{}, retryInterval:{}, partSize:{}, startTime:{}",
                localDirOrFilePath, volumeName, volumePath, totalFilesCount, totalBytes, formatBytes(totalBytes),
                uploadConcurrency, maxRetries, formatDurationMillis(retryIntervalMillis),
                formatPartSize(partSizeBytes), Instant.ofEpochMilli(startTime));

        VolumeSession initialSession;
        try {
            initialSession = refreshVolumeAndClient(volumePath);
        } catch (RuntimeException e) {
            logUploadFailed(localDirOrFilePath, volumePath, startTime);
            throw e;
        }
        try {
            initValidator(localPathPair, initialSession.applyVolumeResponse);
        } catch (RuntimeException e) {
            closeVolumeSession(initialSession);
            logUploadFailed(localDirOrFilePath, volumePath, startTime);
            throw e;
        }
        UploadContext uploadContext = new UploadContext(initialSession);
        ExecutorService uploadExecutor = Executors.newFixedThreadPool(uploadConcurrency);
        UploadProgressTracker progressTracker = new UploadProgressTracker(totalBytes, totalFilesCount, request.getProgressListener());

        return CompletableFuture.allOf(localPathPair.getKey().stream()
                        .map(localFilePath -> CompletableFuture.runAsync(() -> {
                            File file = new File(localFilePath);
                            String progressFilePath = file.getAbsolutePath();
                            long fileStartTime = System.currentTimeMillis();

                            try {
                                uploadLocalFileToVolume(localFilePath, localDirOrFilePath, volumePath, maxRetries, retryIntervalMillis, progressTracker, uploadContext, partSizeBytes);
                                UploadProgressSnapshot progress = progressTracker.finishFile(progressFilePath, file.length());
                                long elapsed = System.currentTimeMillis() - fileStartTime;
                                logger.info("Uploaded file {}/{}: {} ({} bytes) elapsed:{} ms, progress(total bytes): {}/{} bytes, progress(total percentage):{}%, speedBPS:{}, estimatedRemainingTime:{}",
                                        progress.completedFiles, totalFilesCount, localFilePath, file.length(), elapsed,
                                        progress.uploadedBytes, totalBytes, String.format("%.2f", progress.percent),
                                        progress.speedBps, progress.estimatedRemainingTime);
                            } catch (Exception e) {
                                logger.error("Upload failed: {}", localFilePath, e);
                                throw new CompletionException(e);
                            }
                        }, uploadExecutor)).toArray(CompletableFuture[]::new))
                .whenComplete((v, t) -> {
                    uploadExecutor.shutdown();
                    uploadContext.closeSessions();
                    if (t != null) {
                        logUploadFailed(localDirOrFilePath, volumePath, startTime);
                    }
                })
                .thenApply(v -> {
                    progressTracker.finishUpload();
                    long endTime = System.currentTimeMillis();
                    long totalElapsed = endTime - startTime;
                    VolumeSession session = uploadContext.currentSession();
                    logger.info("Volume upload completed: sourcePath:{}, volumeName:{}, volumePath:{}, totalFileCount:{}, totalFileSize:{} bytes ({}), endTime:{}, totalElapsed:{}",
                            localDirOrFilePath, session.applyVolumeResponse.getVolumeName(), volumePath,
                            localPathPair.getKey().size(), localPathPair.getValue(), formatBytes(localPathPair.getValue()),
                            Instant.ofEpochMilli(endTime), formatDurationMillis(totalElapsed));
                    return UploadFilesResult.builder()
                            .volumeName(session.applyVolumeResponse.getVolumeName())
                            .path(volumePath)
                            .build();
                });
    }

    /**
     * Synchronously uploads a local file or directory to the specified path within the Volume.
     */
    public UploadFilesResult uploadFiles(UploadFilesRequest request) throws ExecutionException, InterruptedException {
        return uploadFilesAsync(request).get();
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

    private static String formatPartSize(long partSizeBytes) {
        if (partSizeBytes <= 0L) {
            return "auto";
        }
        return partSizeBytes + " bytes (" + formatBytes(partSizeBytes) + ")";
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        double value = bytes;
        String[] units = new String[]{"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
        int unitIndex = 0;
        while (value >= 1024.0 && unitIndex < units.length - 1) {
            value /= 1024.0;
            unitIndex++;
        }
        return String.format(Locale.ROOT, "%.2f %s", value, units[unitIndex]);
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

    private void initValidator(Pair<List<String>, Long> localPathPair, ApplyVolumeResponse applyVolumeResponse) {
        Long maxContentLength = applyVolumeResponse.getCondition().getMaxContentLength();
        Long uploadFileContentLength = localPathPair.getValue();
        if (uploadFileContentLength > maxContentLength) {
            String msg = String.format("localFileTotalSize %s exceeds the maximum contentLength limit %s defined in the condition. If you are using the free tier, you may switch to the pay-as-you-go volume plan to support uploading larger files.",
                    uploadFileContentLength, maxContentLength);
            logger.error(msg);
            throw new ParamException(msg);
        }

        Long maxFileNumber = applyVolumeResponse.getCondition().getMaxFileNumber();
        int uploadFileNumber = localPathPair.getKey().size();
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

    private void uploadLocalFileToVolume(String localFilePath, String rootPath, String volumePath,
                                         int maxRetries, long retryIntervalMillis,
                                         UploadProgressTracker progressTracker,
                                         UploadContext uploadContext, long partSizeBytes) {
        File file = new File(localFilePath);
        Path filePath = file.toPath().toAbsolutePath();
        Path root = Paths.get(rootPath).toAbsolutePath();

        String relativePath;
        if (root.toFile().isFile()) {
            relativePath = file.getName();
        } else {
            relativePath = root.relativize(filePath).toString().replace("\\", "/");
        }

        VolumeSession session = uploadContext.currentSession();
        String remoteFilePath = session.applyVolumeResponse.getVolumePrefix() + volumePath + relativePath;
        putObjectWithRetry(file, remoteFilePath, volumePath, maxRetries, retryIntervalMillis, progressTracker, uploadContext, partSizeBytes);
    }

    private void putObjectWithRetry(File file, String remoteFilePath, String volumePath,
                                    int maxRetries, long retryIntervalMillis,
                                    UploadProgressTracker progressTracker,
                                    UploadContext uploadContext, long partSizeBytes) {
        String msg = "upload " + file.getAbsolutePath();
        FileUploadProgress progress = new FileUploadProgress(progressTracker, file.getAbsolutePath(), file.length());
        withRetry(msg, () -> {
            progress.reset();
            VolumeSession session = refreshIfExpire(volumePath, uploadContext);
            session.storageClient.putObject(file, session.applyVolumeResponse.getBucketName(), remoteFilePath, progress, partSizeBytes);
            return null;
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
        session.storageClient.close();
    }

    private <T> T withRetry(String actionName, Callable<T> callable, String volumePath,
                            int maxRetries, long retryIntervalMillis,
                            UploadContext uploadContext) {
        int failedAttempts = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
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
                refreshUploadContext(volumePath, uploadContext);
                try {
                    Thread.sleep(retryIntervalMillis);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(actionName + " interrupted while waiting to retry", interruptedException);
                }
            }
        }
    }

    private void withRetry(String actionName, Runnable runnable, String volumePath,
                           int maxRetries, long retryIntervalMillis,
                           UploadContext uploadContext) {
        withRetry(actionName, () -> {
            runnable.run();
            return null;
        }, volumePath, maxRetries, retryIntervalMillis, uploadContext);
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
        return "RequestTimeout".equals(code)
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

    private VolumeSession currentSession() {
        VolumeSession session = lastVolumeSession;
        if (session == null) {
            throw new IllegalStateException("Volume session is not initialized");
        }
        return session;
    }

    private static class UploadContext {
        private final AtomicReference<VolumeSession> sessionRef;
        private final Object refreshLock = new Object();
        private final Set<VolumeSession> sessions = ConcurrentHashMap.newKeySet();

        private UploadContext(VolumeSession session) {
            this.sessionRef = new AtomicReference<>(session);
            this.sessions.add(session);
        }

        private VolumeSession currentSession() {
            VolumeSession session = sessionRef.get();
            if (session == null) {
                throw new IllegalStateException("Volume session is not initialized");
            }
            return session;
        }

        private void setSession(VolumeSession session) {
            sessionRef.set(session);
            sessions.add(session);
        }

        private void closeSessions() {
            for (VolumeSession session : sessions) {
                closeVolumeSession(session);
            }
            sessions.clear();
        }
    }

    private static class UploadProgressTracker {
        private static final long LOG_INTERVAL_MILLIS = 5000L;
        private static final double LOG_PERCENT_STEP = 1.0;

        private final long totalBytes;
        private final long totalFiles;
        private final UploadFilesRequest.ProgressListener progressListener;
        private final long startTimeMillis;
        private final Map<String, Long> fileProgress = new HashMap<>();
        private final Set<String> completedFiles = new HashSet<>();
        private long uploadedBytes = 0L;
        private long lastLogTimeMillis = 0L;
        private double lastLoggedPercent = -1.0;

        private UploadProgressTracker(long totalBytes, long totalFiles,
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
            UploadProgress progress = null;
            synchronized (this) {
                if (chunkBytes <= 0) {
                    return;
                }
                long previous = fileProgress.getOrDefault(filePath, 0L);
                long current = Math.min(fileSize, previous + chunkBytes);
                long delta = current - previous;
                if (delta <= 0) {
                    return;
                }
                fileProgress.put(filePath, current);
                uploadedBytes += delta;
                progress = progressIfNeeded(filePath, current, fileSize);
            }
            if (progress != null) {
                emitProgress(progress);
            }
        }

        UploadProgressSnapshot finishFile(String filePath, long fileSize) {
            UploadProgress progress;
            UploadProgressSnapshot snapshot;
            synchronized (this) {
                long previous = fileProgress.getOrDefault(filePath, 0L);
                long current = Math.max(previous, fileSize);
                fileProgress.put(filePath, current);
                uploadedBytes += current - previous;
                completedFiles.add(filePath);
                double percent = percent();
                progress = snapshot(filePath, current, fileSize, percent);
                long speedBps = speedBps(uploadedBytes, System.currentTimeMillis());
                snapshot = new UploadProgressSnapshot(uploadedBytes, completedFiles.size(), percent,
                        speedBps, formatDurationMillis(estimatedRemainingTimeMillis(uploadedBytes, speedBps)));
                markProgressEmitted(percent);
            }
            emitProgress(progress);
            return snapshot;
        }

        void finishUpload() {
            UploadProgress progress;
            synchronized (this) {
                progress = snapshot("", 0L, 0L, percent());
                markProgressEmitted(progress.getPercent());
            }
            emitProgress(progress);
        }

        private double percent() {
            if (totalBytes == 0) {
                return 100.0;
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

        private UploadProgress progressIfNeeded(String currentFile, long currentFileUploadedBytes, long currentFileTotalBytes) {
            long now = System.currentTimeMillis();
            double percent = percent();
            if (percent - lastLoggedPercent >= LOG_PERCENT_STEP || now - lastLogTimeMillis >= LOG_INTERVAL_MILLIS) {
                lastLogTimeMillis = now;
                lastLoggedPercent = percent;
                return snapshot(currentFile, currentFileUploadedBytes, currentFileTotalBytes, percent);
            }
            return null;
        }

        private UploadProgress snapshot(String currentFile, long currentFileUploadedBytes,
                                        long currentFileTotalBytes, double percent) {
            return new UploadProgress(uploadedBytes, totalBytes, completedFiles.size(), totalFiles,
                    currentFile, currentFileUploadedBytes, currentFileTotalBytes, percent);
        }

        private void markProgressEmitted(double percent) {
            lastLogTimeMillis = System.currentTimeMillis();
            lastLoggedPercent = percent;
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

    private static class UploadProgressSnapshot {
        private final long uploadedBytes;
        private final int completedFiles;
        private final double percent;
        private final long speedBps;
        private final String estimatedRemainingTime;

        private UploadProgressSnapshot(long uploadedBytes, int completedFiles, double percent,
                                       long speedBps, String estimatedRemainingTime) {
            this.uploadedBytes = uploadedBytes;
            this.completedFiles = completedFiles;
            this.percent = percent;
            this.speedBps = speedBps;
            this.estimatedRemainingTime = estimatedRemainingTime;
        }
    }

    private static class VolumeSession {
        private final ApplyVolumeResponse applyVolumeResponse;
        private final StorageClient storageClient;
        private final Instant expireTime;

        private VolumeSession(ApplyVolumeResponse applyVolumeResponse, StorageClient storageClient, Instant expireTime) {
            this.applyVolumeResponse = applyVolumeResponse;
            this.storageClient = storageClient;
            this.expireTime = expireTime;
        }
    }

}
