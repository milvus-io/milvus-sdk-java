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
import io.milvus.bulkwriter.common.utils.FileUtils;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.request.stage.ApplyStageRequest;
import io.milvus.bulkwriter.request.stage.UploadFilesRequest;
import io.milvus.bulkwriter.response.ApplyStageResponse;
import io.milvus.bulkwriter.restful.DataStageUtils;
import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.bulkwriter.storage.client.MinioStorageClient;
import io.milvus.exception.ParamException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StageFileManager {
    private static final Logger logger = LoggerFactory.getLogger(StageFileManager.class);
    private final String cloudEndpoint;
    private final String apiKey;
    private final String stageName;
    private final ExecutorService executor;

    private StorageClient storageClient;
    private ApplyStageResponse applyStageResponse;

    public StageFileManager(StageFileManagerParam stageWriterParam) {
        this.cloudEndpoint = stageWriterParam.getCloudEndpoint();
        this.apiKey = stageWriterParam.getApiKey();
        this.stageName = stageWriterParam.getStageName();
        this.executor = Executors.newFixedThreadPool(20);
    }

    /**
     * Asynchronously uploads a local file or directory to the specified path within the Stage.
     *
     * @param request the upload request containing the source local file or directory path
     *                and the target directory path in the Stage {@link UploadFilesRequest}
     * @return a {@link CompletableFuture} that completes with an {@link UploadFilesResult}
     * once all files have been uploaded successfully
     * @throws CompletionException if an error occurs during the upload process
     */
    public CompletableFuture<UploadFilesResult> uploadFilesAsync(UploadFilesRequest request) {
        String localDirOrFilePath = request.getSourceFilePath();
        Pair<List<String>, Long> localPathPair = FileUtils.processLocalPath(localDirOrFilePath);
        String stagePath = convertDirPath(request.getTargetStagePath());

        refreshStageAndClient(stagePath);
        initValidator(localPathPair);

        AtomicInteger currentFileCount = new AtomicInteger(0);
        AtomicLong processedBytes = new AtomicLong(0);
        long totalBytes = localPathPair.getValue();
        long totalFilesCount = localPathPair.getKey().size();
        long startTime = System.currentTimeMillis();

        return CompletableFuture.allOf(localPathPair.getKey().stream()
                        .map(localFilePath -> CompletableFuture.runAsync(() -> {
                            File file = new File(localFilePath);
                            long fileStartTime = System.currentTimeMillis();

                            try {
                                uploadLocalFileToStage(localFilePath, localDirOrFilePath, stagePath);
                                long bytes = processedBytes.addAndGet(file.length());
                                int completeCount = currentFileCount.incrementAndGet();
                                long elapsed = System.currentTimeMillis() - fileStartTime;
                                double percent = totalBytes == 0 ? 100.0 : (bytes * 100.0 / totalBytes);
                                logger.info("Uploaded file {}/{}: {} ({} bytes) elapsed:{} ms, progress(total bytes): {}/{} bytes, progress(total percentage):{}%",
                                        completeCount, totalFilesCount, localFilePath, file.length(), elapsed, bytes, totalBytes, String.format("%.2f", percent));
                            } catch (Exception e) {
                                logger.error("Upload failed: {}", localFilePath, e);
                                throw new CompletionException(e);
                            }
                        }, executor)).toArray(CompletableFuture[]::new))
                .whenComplete((v, t) -> {
                })
                .thenApply(v -> {
                    long totalElapsed = (System.currentTimeMillis() - startTime) / 1000;
                    logger.info("all files in {} has been async uploaded to stage, stageName:{}, stagePath:{}, totalFileCount:{}, totalFileSize:{}, cost times:{} s",
                            localDirOrFilePath, applyStageResponse.getStageName(), stagePath, localPathPair.getKey().size(), localPathPair.getValue(), totalElapsed);
                    return UploadFilesResult.builder()
                            .stageName(applyStageResponse.getStageName())
                            .path(stagePath)
                            .build();
                });
    }

    /**
     * Gracefully shuts down the internal thread pool executor.
     * <p>
     * This method attempts to stop accepting new tasks and waits for existing
     * tasks to complete within a timeout period. If tasks do not finish within
     * the timeout, it will forcefully shut down the executor.
     * </p>
     *
     * Usage recommendation:
     * <ul>
     *   <li>Call this method when the StageFileManager is no longer needed.</li>
     * </ul>
     *
     * Thread interruption is respected, and the interrupt status is restored if interrupted during shutdown.
     */
    public void shutdownGracefully() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("Executor didn't terminate in time, forcing shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for executor to shutdown", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void initValidator(Pair<List<String>, Long> localPathPair) {
        if (localPathPair.getValue() > applyStageResponse.getCondition().getMaxContentLength()) {
            String msg = String.format("localFileTotalSize %s exceeds the maximum contentLength limit %s defined in the condition. If you want to upload larger files, please contact us to lift the restriction", localPathPair.getValue(), applyStageResponse.getCondition().getMaxContentLength());
            logger.error(msg);
            throw new ParamException(msg);
        }
    }

    private void refreshStageAndClient(String path) {
        logger.info("refreshing Stage info...");
        ApplyStageRequest applyStageRequest = ApplyStageRequest.builder()
                .apiKey(apiKey)
                .stageName(stageName)
                .path(path)
                .build();
        String result = DataStageUtils.applyStage(cloudEndpoint, applyStageRequest);
        applyStageResponse = new Gson().fromJson(result, ApplyStageResponse.class);
        logger.info("stage info refreshed");

        storageClient = MinioStorageClient.getStorageClient(
                applyStageResponse.getCloud(),
                applyStageResponse.getEndpoint(),
                applyStageResponse.getCredentials().getTmpAK(),
                applyStageResponse.getCredentials().getTmpSK(),
                applyStageResponse.getCredentials().getSessionToken(),
                applyStageResponse.getRegion(), null);
        logger.info("storage client refreshed");
    }

    private String convertDirPath(String inputPath) {
        if (StringUtils.isEmpty(inputPath) || inputPath.equals("/")) {
            return "";
        }
        if (inputPath.endsWith("/")) {
            return inputPath;
        }
        return inputPath + "/";
    }

    private void uploadLocalFileToStage(String localFilePath, String rootPath, String stagePath) {
        File file = new File(localFilePath);
        Path filePath = file.toPath().toAbsolutePath();
        Path root = Paths.get(rootPath).toAbsolutePath();

        String relativePath;
        if (root.toFile().isFile()) {
            relativePath = file.getName();
        } else {
            relativePath = root.relativize(filePath).toString().replace("\\", "/");
        }

        String remoteFilePath = applyStageResponse.getStagePrefix() + stagePath + relativePath;
        putObjectWithRetry(file, remoteFilePath, stagePath);
    }

    private void putObjectWithRetry(File file, String remoteFilePath, String stagePath) {
        refreshIfExpire(stagePath);
        String msg = "upload " + file.getAbsolutePath();
        withRetry(msg, () -> {
            try {
                storageClient.putObject(file, applyStageResponse.getBucketName(), remoteFilePath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, stagePath);

    }

    private void refreshIfExpire(String stagePath) {
        Instant instant = Instant.parse(applyStageResponse.getCredentials().getExpireTime());
        Date expireTime = Date.from(instant);
        if (new Date().after(expireTime)) {
            synchronized (this) {
                if (new Date().after(expireTime)) {
                    refreshStageAndClient(stagePath);
                }
            }
        }
    }

    private <T> T withRetry(String actionName, Callable<T> callable, String stagePath) {
        final int maxRetries = 5;
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                return callable.call();
            } catch (Exception e) {
                attempt++;
                refreshStageAndClient(stagePath);
                logger.warn("Attempt {} failed to {}", attempt, actionName, e);
                if (attempt == maxRetries) {
                    throw new RuntimeException(actionName + " failed after " + maxRetries + " attempts", e);
                }
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ignored) {
                }
            }
        }
        throw new RuntimeException(actionName + " failed unexpectedly.");
    }

    private void withRetry(String actionName, Runnable runnable, String stagePath) {
        withRetry(actionName, () -> {
            runnable.run();
            return null;
        }, stagePath);
    }


}