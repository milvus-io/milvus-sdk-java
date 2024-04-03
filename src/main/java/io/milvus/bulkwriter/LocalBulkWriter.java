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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.param.collection.CollectionSchemaParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LocalBulkWriter extends BulkWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LocalBulkWriter.class);
    protected String localPath;
    private String uuid;
    private int flushCount;
    private Map<String, Thread> workingThread;
    private ReentrantLock workingThreadLock;
    private List<List<String>> localFiles;

    public LocalBulkWriter(LocalBulkWriterParam bulkWriterParam) throws IOException {
        super(bulkWriterParam.getCollectionSchema(), bulkWriterParam.getChunkSize(), bulkWriterParam.getFileType());
        this.localPath = bulkWriterParam.getLocalPath();
        this.uuid = UUID.randomUUID().toString();
        this.workingThreadLock = new ReentrantLock();
        this.workingThread = new HashMap<>();
        this.localFiles = Lists.newArrayList();
        this.makeDir();
    }

    protected LocalBulkWriter(CollectionSchemaParam collectionSchema, int chunkSize, BulkFileType fileType, String localPath) throws IOException {
        super(collectionSchema, chunkSize, fileType);
        this.localPath = localPath;
        this.uuid = UUID.randomUUID().toString();
        this.workingThreadLock = new ReentrantLock();
        this.workingThread = new HashMap<>();
        this.localFiles = Lists.newArrayList();
        this.makeDir();
    }

    public void appendRow(JSONObject rowData) throws IOException, InterruptedException {
        super.appendRow(rowData);

//        only one thread can enter this section to persist data,
//        in the _flush() method, the buffer will be swapped to a new one.
//        in async mode, the flush thread is asynchronously, other threads can
//        continue to append if the new buffer size is less than target size
        workingThreadLock.lock();
        if (super.getBufferSize() > super.getChunkSize()) {
            commit(true);
        }
        workingThreadLock.unlock();
    }

    public void commit(boolean async) throws InterruptedException {
        // _async=True, the flush thread is asynchronously
        while (workingThread.size() > 0) {
            String msg = String.format("Previous flush action is not finished, %s is waiting...", Thread.currentThread().getName());
            logger.info(msg);
            TimeUnit.SECONDS.sleep(5);
        }

        String msg = String.format("Prepare to flush buffer, row_count: %s, size: %s", super.getBufferRowCount(), super.getBufferSize());
        logger.info(msg);

        int bufferRowCount = getBufferRowCount();
        int bufferSize = getBufferSize();
        Runnable runnable = () -> flush(bufferSize, bufferRowCount);
        Thread thread = new Thread(runnable);
        logger.info("Flush thread begin, name: {}", thread.getName());
        workingThread.put(thread.getName(), thread);
        thread.start();

        if (!async) {
            logger.info("Wait flush to finish");
            thread.join();
        }

        // reset the buffer size
        super.commit(false);
        logger.info("Commit done with async={}", async);
    }

    private void flush(Integer bufferSize, Integer bufferRowCount) {
        flushCount += 1;
        java.nio.file.Path path = Paths.get(localPath);
        java.nio.file.Path flushDirPath = path.resolve(String.valueOf(flushCount));

        Buffer oldBuffer = super.newBuffer();
        if (oldBuffer.getRowCount() > 0) {
            List<String> fileList = oldBuffer.persist(
                    flushDirPath.toString(), bufferSize, bufferRowCount
            );
            localFiles.add(fileList);
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

    private void makeDir() throws IOException {
        java.nio.file.Path path = Paths.get(localPath);
        createDirIfNotExist(path);

        java.nio.file.Path fullPath = path.resolve(uuid);
        createDirIfNotExist(fullPath);
        this.localPath = fullPath.toString();
    }

    private void createDirIfNotExist(java.nio.file.Path path) throws IOException {
        try {
            Files.createDirectories(path);
            logger.info("Data path created: {}", path);
        } catch (IOException e) {
            logger.error("Data Path create failed: {}", path);
            throw e;
        }
    }

    protected void exit() throws InterruptedException {
        // if still has data in memory, default commit
        workingThreadLock.lock();
        if (getBufferSize() != null && getBufferSize() != 0) {
            commit(true);
        }
        workingThreadLock.unlock();

        // wait flush thread
        if (workingThread.size() > 0) {
            for (String key : workingThread.keySet()) {
                logger.info("Wait flush thread '{}' to finish", key);
                workingThread.get(key).join();
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
