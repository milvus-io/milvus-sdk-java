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

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.request.stage.UploadFilesRequest;
import io.milvus.common.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class StageBulkWriter extends LocalBulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(StageBulkWriter.class);

    private String remotePath;
    private List<List<String>> remoteFiles;
    private StageFileManager stageFileManager;
    private StageBulkWriterParam stageBulkWriterParam;

    public StageBulkWriter(StageBulkWriterParam bulkWriterParam) throws IOException {
        super(bulkWriterParam.getCollectionSchema(),
                bulkWriterParam.getChunkSize(),
                bulkWriterParam.getFileType(),
                generatorLocalPath(),
                bulkWriterParam.getConfig());
        Path path = Paths.get(bulkWriterParam.getRemotePath());
        Path remoteDirPath = path.resolve(getUUID());
        this.remotePath = remoteDirPath + "/";
        this.stageFileManager = initStageFileManagerParams(bulkWriterParam);
        this.stageBulkWriterParam = bulkWriterParam;

        this.remoteFiles = Lists.newArrayList();
        logger.info("Remote buffer writer initialized, target path: {}", remotePath);

    }

    private StageFileManager initStageFileManagerParams(StageBulkWriterParam bulkWriterParam) throws IOException {
        StageFileManagerParam stageFileManagerParam = StageFileManagerParam.newBuilder()
                .withCloudEndpoint(bulkWriterParam.getCloudEndpoint()).withApiKey(bulkWriterParam.getApiKey())
                .withStageName(bulkWriterParam.getStageName())
                .build();
        return new StageFileManager(stageFileManagerParam);
    }

    @Override
    public void appendRow(JsonObject rowData) throws IOException, InterruptedException {
        super.appendRow(rowData);
    }

    @Override
    public void commit(boolean async) throws InterruptedException {
        super.commit(async);
    }

    @Override
    protected String getDataPath() {
        return remotePath;
    }

    @Override
    public List<List<String>> getBatchFiles() {
        return remoteFiles;
    }

    public UploadFilesResult getStageUploadResult() {
        return UploadFilesResult.builder()
                .stageName(stageBulkWriterParam.getStageName())
                .path(remotePath)
                .build();
    }

    @Override
    protected void exit() throws InterruptedException {
        super.exit();
        // remove the temp folder "bulk_writer"
        Path parentPath = Paths.get(localPath).getParent();
        if (parentPath.toFile().exists() && isEmptyDirectory(parentPath)) {
            try {
                Files.delete(parentPath);
                logger.info("Delete empty directory: " + parentPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isEmptyDirectory(Path directory) {
        try {
            return !Files.walk(directory, 1, FileVisitOption.FOLLOW_LINKS)
                    .skip(1) // Skip the root directory itself
                    .findFirst()
                    .isPresent();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void rmLocal(String file) {
        try {
            Path filePath = Paths.get(file);
            filePath.toFile().delete();

            Path parentDir = filePath.getParent();
            if (parentDir != null && !parentDir.toString().equals(localPath)) {
                try {
                    Files.delete(parentDir);
                    logger.info("Delete empty directory: " + parentDir);
                } catch (IOException ex) {
                    logger.warn("Failed to delete empty directory: " + parentDir);
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to delete local file: " + file);
        }
    }

    @Override
    protected void callBack(List<String> fileList) {
        serialImportData(fileList);
    }

    @Override
    public void close() throws Exception {
        logger.info("execute remaining actions to prevent loss of memory data or residual empty directories.");
        exit();
        logger.info(String.format("RemoteBulkWriter done! output remote files: %s", getBatchFiles()));
    }

    private void serialImportData(List<String> fileList) {
        List<String> remoteFileList = new ArrayList<>();
        try {
            for (String filePath : fileList) {
                String relativeFilePath = filePath.replace(super.getDataPath(), "");
                String minioFilePath = getMinioFilePath(remotePath, relativeFilePath);

                uploadObject(filePath, minioFilePath);
                remoteFileList.add(minioFilePath);
                rmLocal(filePath);
            }

        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to upload files, error: %s", e));
        }

        logger.info("Successfully upload files: " + fileList);
        remoteFiles.add(remoteFileList);
    }

    private void uploadObject(String filePath, String objectName) throws Exception {
        logger.info(String.format("Prepare to upload %s to %s", filePath, objectName));

        UploadFilesRequest uploadFilesRequest = UploadFilesRequest.builder()
                .sourceFilePath(filePath).targetStagePath(remotePath)
                .build();

        stageFileManager.uploadFilesAsync(uploadFilesRequest).get();
        logger.info(String.format("Upload file %s to %s", filePath, objectName));

    }

    private static String generatorLocalPath() {
        Path currentWorkingDirectory = Paths.get("").toAbsolutePath();
        Path currentScriptPath = currentWorkingDirectory.resolve("bulk_writer");
        return currentScriptPath.toString();
    }

    private static String getMinioFilePath(String remotePath, String relativeFilePath) {
        remotePath = remotePath.startsWith("/") ? remotePath.substring(1) : remotePath;
        Path remote = Paths.get(remotePath);

        relativeFilePath = relativeFilePath.startsWith("/") ? relativeFilePath.substring(1) : relativeFilePath;
        Path relative = Paths.get(relativeFilePath);
        Path joinedPath = remote.resolve(relative);
        return joinedPath.toString();
    }
}
