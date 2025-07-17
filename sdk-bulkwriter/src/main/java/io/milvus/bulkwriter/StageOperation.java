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
import io.milvus.bulkwriter.model.StageUploadResult;
import io.milvus.bulkwriter.request.stage.ApplyStageRequest;
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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StageOperation {
    private static final Logger logger = LoggerFactory.getLogger(StageOperation.class);
    private final String cloudEndpoint;
    private final String apiKey;
    private final String stageName;
    private Pair<List<String>, Long> localPathPair;
    private final String path;

    private StorageClient storageClient;
    private ApplyStageResponse applyStageResponse;

    public StageOperation(StageOperationParam stageWriterParam) throws IOException {
        cloudEndpoint = stageWriterParam.getCloudEndpoint();
        apiKey = stageWriterParam.getApiKey();
        stageName = stageWriterParam.getStageName();
        path = convertDirPath(stageWriterParam.getPath());

        refreshStageAndClient();
    }

    public StageUploadResult uploadFileToStage(String localDirOrFilePath) throws Exception {
        localPathPair = FileUtils.processLocalPath(localDirOrFilePath);
        initValidator();

        logger.info("begin to upload file to stage, localDirOrFilePath:{}, fileCount:{} to stageName:{}, stagePath:{}", localDirOrFilePath, localPathPair.getKey().size(), applyStageResponse.getStageName(), path);
        long startTime = System.currentTimeMillis();

        int concurrency = 20; // 并发线程数
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        AtomicInteger currentFileCount = new AtomicInteger(0);
        long totalFiles = localPathPair.getKey().size();
        AtomicLong processedBytes = new AtomicLong(0);
        long totalBytes = localPathPair.getValue();

        List<Future<?>> futures = new ArrayList<>();
        for (String localFilePath : localPathPair.getKey()) {
            futures.add(executor.submit(() -> {
                long current = currentFileCount.incrementAndGet();
                File file = new File(localFilePath);
                long fileStartTime = System.currentTimeMillis();
                try {
                    uploadLocalFileToStage(localFilePath, localDirOrFilePath);
                    long bytes = processedBytes.addAndGet(file.length());
                    long elapsed = System.currentTimeMillis() - fileStartTime;
                    double percent = totalBytes == 0 ? 100.0 : (bytes * 100.0 / totalBytes);
                    logger.info("Uploaded file {}/{}: {} ({} bytes) elapsed:{} ms, progress(total bytes): {}/{} bytes, progress(total percentage):{}%",
                            current, totalFiles, localFilePath, file.length(), elapsed, bytes, totalBytes, String.format("%.2f", percent));
                } catch (Exception e) {
                    logger.error("Upload failed for file: {}", localFilePath, e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        long totalElapsed = (System.currentTimeMillis() - startTime) / 1000;
        logger.info("all files in {} has been uploaded to stage, stageName:{}, stagePath:{}, totalFileCount:{}, totalFileSize:{}, cost times:{} s",
                localDirOrFilePath, applyStageResponse.getStageName(), path, localPathPair.getKey().size(), localPathPair.getValue(), totalElapsed);
        return StageUploadResult.builder().stageName(applyStageResponse.getStageName()).path(path).build();
    }

    private void initValidator() {
        if (localPathPair.getValue() > applyStageResponse.getCondition().getMaxContentLength()) {
            String msg = String.format("localFileTotalSize %s exceeds the maximum contentLength limit %s defined in the condition. If you want to upload larger files, please contact us to lift the restriction", localPathPair.getValue(), applyStageResponse.getCondition().getMaxContentLength());
            logger.error(msg);
            throw new ParamException(msg);
        }
    }

    private void refreshStageAndClient() {
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
        if (StringUtils.isEmpty(inputPath) || inputPath.endsWith("/")) {
            return inputPath;
        }
        return inputPath + "/";
    }

    private String uploadLocalFileToStage(String localFilePath, String rootPath) throws Exception {
        File file = new File(localFilePath);
        Path filePath = file.toPath().toAbsolutePath();
        Path root = Paths.get(rootPath).toAbsolutePath();

        String relativePath;
        if (root.toFile().isFile()) {
            relativePath = file.getName();
        } else {
            relativePath = root.relativize(filePath).toString().replace("\\", "/");
        }

        String remoteFilePath = applyStageResponse.getUploadPath() + relativePath;
        putObject(file, remoteFilePath);
        return remoteFilePath;
    }

    private void putObject(File file, String remoteFilePath) throws Exception {
        Instant instant = Instant.parse(applyStageResponse.getCredentials().getExpireTime());
        Date expireTime = Date.from(instant);
        if (new Date().after(expireTime)) {
            synchronized (this) {
                if (new Date().after(expireTime)) {
                    refreshStageAndClient();
                }
            }
        }
        uploadWithRetry(file, remoteFilePath);
    }

    private void uploadWithRetry(File file, String remoteFilePath) {
        final int maxRetries = 3;
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                storageClient.putObject(file, applyStageResponse.getBucketName(), remoteFilePath);
                return;
            } catch (Exception e) {
                attempt++;
                refreshStageAndClient();
                logger.warn("Attempt {} failed to upload {}", attempt, file.getAbsolutePath(), e);
                if (attempt == maxRetries) {
                    throw new RuntimeException("Upload failed after " + maxRetries + " attempts", e);
                }
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}