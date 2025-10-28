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
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.writer.FormatFileWriter;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LocalBulkWriter extends BulkWriter {
    private static final Logger logger = LoggerFactory.getLogger(LocalBulkWriter.class);

    private final Map<String, Thread> workingThread;
    private final ReentrantLock workingThreadLock;
    private final List<List<String>> localFiles;

    public LocalBulkWriter(LocalBulkWriterParam bulkWriterParam) throws IOException {
        super(bulkWriterParam.getCollectionSchema(), bulkWriterParam.getChunkSize(), bulkWriterParam.getFileType(), bulkWriterParam.getLocalPath(), bulkWriterParam.getConfig());
        this.workingThreadLock = new ReentrantLock();
        this.workingThread = new HashMap<>();
        this.localFiles = Lists.newArrayList();
    }

    protected LocalBulkWriter(CreateCollectionReq.CollectionSchema collectionSchema,
                              long chunkSize,
                              BulkFileType fileType,
                              String localPath,
                              Map<String, Object> config) throws IOException {
        super(collectionSchema, chunkSize, fileType, localPath, config);
        this.workingThreadLock = new ReentrantLock();
        this.workingThread = new HashMap<>();
        this.localFiles = Lists.newArrayList();
    }

    @Override
    public void appendRow(JsonObject rowData) throws IOException, InterruptedException {
        super.appendRow(rowData);
    }

    @Override
    protected void callBackIfCommitReady(List<String> filePaths) throws InterruptedException {
//        only one thread can enter this section to persist data,
//        in the _flush() method, the buffer will be swapped to a new one.
//        in async mode, the flush thread is asynchronously, other threads can
//        continue to append if the new buffer size is less than target size
        workingThreadLock.lock();
        callBack(true, filePaths);
        workingThreadLock.unlock();
    }

    public void commit(boolean async) throws InterruptedException {
        List<String> filePath = commitIfFileReady(false);
        callBack(async, filePath);
    }

    protected List<String> commitIfFileReady(boolean createNewFile) {
        if (super.getTotalRowCount() <= 0) {
            String msg = "current_file_total_row_count less than 0, no need to generator a file";
            logger.info(msg);
            return null;
        }

        String filePath = super.getFileWriter().getFilePath();
        String msg = String.format("Prepare to commit file:%s, current_file_total_row_count: %s, current_file_total_size:%s, create_new_file:%s",
                filePath, super.getTotalRowCount(), super.getTotalSize(), createNewFile);
        logger.info(msg);

        List<String> fileList = Lists.newArrayList(filePath);
        try {
            FormatFileWriter oldFileWriter = createNewFile ? this.newFileWriter() : super.getFileWriter();
            oldFileWriter.close();

            localFiles.add(fileList);
            // reset the total size and count
            super.commit();
        } catch (IOException e) {
            // this function is running in a thread
            // TODO: interrupt main thread if failed to persist file
            logger.error(e.getMessage());
        }
        return fileList;
    }

    private void callBack(boolean async, List<String> fileList) throws InterruptedException {
        if (CollectionUtils.isEmpty(fileList)) {
            return;
        }

        // _async=True, the flush thread is asynchronously
        while (!workingThread.isEmpty()) {
            String msg = String.format("Previous callBack action is not finished, %s is waiting...", Thread.currentThread().getName());
            logger.info(msg);
            TimeUnit.SECONDS.sleep(5);
        }

        String msg = String.format("Prepare to callBack, async:%s, fileList:%s", async, fileList);
        logger.info(msg);

        Runnable runnable = () -> commitIfFileReady(fileList);
        Thread thread = new Thread(runnable);
        logger.info("CallBack thread begin, name: {}", thread.getName());
        workingThread.put(thread.getName(), thread);
        thread.start();

        if (!async) {
            logger.info("Wait callBack to finish");
            thread.join();
        }

        logger.info("CallBack done with async={}", async);
    }

    private void commitIfFileReady(List<String> fileList) {
        if (CollectionUtils.isNotEmpty(fileList)) {
            callBack(fileList);
        }
        workingThread.remove(Thread.currentThread().getName());
        String msg = String.format("Flush thread done, name: %s", Thread.currentThread().getName());
        logger.info(msg);
    }

    protected void callBack(List<String> fileList) {
    }

    @Override
    protected String getDataPath() {
        return localPath;
    }

    public List<List<String>> getBatchFiles() {
        return localFiles;
    }

    protected void exit() throws InterruptedException {
        // if still has data in memory, default commit
        workingThreadLock.lock();

        List<String> filePath = commitIfFileReady(false);
        callBack(true, filePath);
        workingThreadLock.unlock();

        // wait flush thread
        if (!workingThread.isEmpty()) {
            for (String key : workingThread.keySet()) {
                logger.info("Wait flush thread '{}' to finish", key);
                Thread thread = workingThread.get(key);
                if (thread != null) {
                    thread.join();
                }
            }
        }

        rmDir();
    }

    private void rmDir() {
        try {
            java.nio.file.Path path = Paths.get(localPath);
            if (Files.exists(path) && isDirectoryEmpty(path)) {
                Files.delete(path);
                logger.info("Delete local directory {}", localPath);
            }
        } catch (IOException e) {
            logger.error("Error while deleting directory: " + e.getMessage());
        }
    }

    private boolean isDirectoryEmpty(java.nio.file.Path path) throws IOException {
        try (DirectoryStream<java.nio.file.Path> dirStream = Files.newDirectoryStream(path)) {
            return !dirStream.iterator().hasNext();
        }
    }

    protected String getUUID() {
        return uuid;
    }

    @Override
    public void close() throws Exception {
        logger.info("execute remaining actions to prevent loss of memory data or residual empty directories.");
        exit();
        logger.info(String.format("LocalBulkWriter done! output local files: %s", getBatchFiles()));
    }
}
