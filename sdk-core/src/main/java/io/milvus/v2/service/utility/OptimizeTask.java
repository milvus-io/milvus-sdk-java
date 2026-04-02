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

package io.milvus.v2.service.utility;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.response.OptimizeResp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OptimizeTask {

    public enum ProgressStage {
        INITIALIZING("initializing"),
        WAITING_FOR_INDEXES("waiting for indexes"),
        COMPACTING("compacting"),
        WAITING_FOR_COMPACTION("waiting for compaction"),
        WAITING_FOR_INDEX_REBUILD("waiting for index rebuild"),
        REFRESHING_LOAD("refreshing load"),
        CANCELLED("cancelled");

        private final String description;

        ProgressStage(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    @FunctionalInterface
    public interface ExecuteFn {
        OptimizeResp execute(OptimizeTask task, String collectionName, String databaseName,
                             Long sizeMb, Long timeout);
    }

    private static final Pattern SIZE_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([a-z]*)$");
    private static final Map<String, Long> UNIT_TO_BYTES = new HashMap<>();

    static {
        UNIT_TO_BYTES.put("b", 1L);
        UNIT_TO_BYTES.put("kb", 1024L);
        UNIT_TO_BYTES.put("mb", 1024L * 1024);
        UNIT_TO_BYTES.put("gb", 1024L * 1024 * 1024);
        UNIT_TO_BYTES.put("tb", 1024L * 1024 * 1024 * 1024);
        UNIT_TO_BYTES.put("pb", 1024L * 1024 * 1024 * 1024 * 1024);
    }

    private final String collectionName;
    private final String databaseName;
    private final String targetSize;
    private final Long sizeMb;
    private final Long timeout;
    private final ExecuteFn executeFn;
    private final Thread thread;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition doneCondition = lock.newCondition();
    private volatile boolean done = false;
    private volatile boolean cancelled = false;
    private OptimizeResp result;
    private Exception exception;
    private ProgressStage progressStage = ProgressStage.INITIALIZING;
    private final List<ProgressStage> progressHistory = new ArrayList<>();

    public OptimizeTask(String collectionName, String databaseName, String targetSize,
                        Long timeout, ExecuteFn executeFn) {
        this.collectionName = collectionName;
        this.databaseName = databaseName;
        this.targetSize = targetSize;
        this.sizeMb = parseTargetSize(targetSize);
        this.timeout = timeout;
        this.executeFn = executeFn;
        this.progressHistory.add(ProgressStage.INITIALIZING);

        this.thread = new Thread(this::run, "milvus-optimize-" + collectionName);
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        try {
            OptimizeResp resp = executeFn.execute(this, collectionName, databaseName, sizeMb, timeout);
            if (!cancelled) {
                setResult(resp);
            }
        } catch (Exception e) {
            if (!cancelled) {
                setException(e);
            }
        }
    }

    public boolean isDone() {
        lock.lock();
        try {
            return done;
        } finally {
            lock.unlock();
        }
    }

    public boolean isCancelled() {
        lock.lock();
        try {
            return cancelled;
        } finally {
            lock.unlock();
        }
    }

    public ProgressStage getProgress() {
        lock.lock();
        try {
            return progressStage;
        } finally {
            lock.unlock();
        }
    }

    public List<ProgressStage> getProgressHistory() {
        lock.lock();
        try {
            return new ArrayList<>(progressHistory);
        } finally {
            lock.unlock();
        }
    }

    public List<String> getProgressHistoryAsStrings() {
        lock.lock();
        try {
            List<String> result = new ArrayList<>();
            for (ProgressStage stage : progressHistory) {
                result.add(stage.getDescription());
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public boolean cancel() {
        lock.lock();
        try {
            if (cancelled) {
                return true;
            }
            if (done) {
                return false;
            }
            cancelled = true;
            progressStage = ProgressStage.CANCELLED;
            progressHistory.add(ProgressStage.CANCELLED);
            exception = new MilvusClientException(ErrorCode.CLIENT_ERROR, "Optimization task was cancelled");
            done = true;
            doneCondition.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void checkCancelled() {
        lock.lock();
        try {
            if (cancelled) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, "Optimization task was cancelled");
            }
        } finally {
            lock.unlock();
        }
    }

    public OptimizeResp getResult(Long timeoutMs) {
        lock.lock();
        try {
            if (!done) {
                if (timeoutMs != null) {
                    long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
                    while (!done && remainingNanos > 0) {
                        remainingNanos = doneCondition.awaitNanos(remainingNanos);
                    }
                } else {
                    while (!done) {
                        doneCondition.await();
                    }
                }
            }

            if (!done) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR,
                        "Timeout waiting for optimization to complete");
            }

            if (exception != null) {
                if (exception instanceof MilvusClientException) {
                    throw (MilvusClientException) exception;
                }
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR,
                        "Optimization failed: " + exception.getMessage());
            }

            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR,
                    "Interrupted while waiting for optimization result");
        } finally {
            lock.unlock();
        }
    }

    public void setProgress(ProgressStage stage) {
        lock.lock();
        try {
            if (cancelled) {
                return;
            }
            progressStage = stage;
            progressHistory.add(stage);
        } finally {
            lock.unlock();
        }
    }

    private void setResult(OptimizeResp result) {
        lock.lock();
        try {
            this.result = result;
            this.done = true;
            doneCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void setException(Exception exception) {
        lock.lock();
        try {
            this.exception = exception;
            this.done = true;
            doneCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Parse target size string and return size in MB.
     *
     * @param targetSize size string like "512MB", "1GB", "1.5gb", or null
     * @return size in MB, or null if targetSize is null
     */
    static Long parseTargetSize(String targetSize) {
        if (targetSize == null) {
            return null;
        }

        String trimmed = targetSize.trim().toLowerCase();
        Matcher matcher = SIZE_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Invalid target_size format: '" + targetSize + "'. " +
                            "Expected format: '1000MB', '1GB', '1.2gb', '500KB'");
        }

        try {
            double value = Double.parseDouble(matcher.group(1));
            if (!Double.isFinite(value) || value <= 0) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Invalid target_size value: '" + targetSize + "', must be a positive finite number");
            }
            String unit = matcher.group(2);
            if (unit.isEmpty()) {
                unit = "b";
            }

            Long bytesPerUnit = UNIT_TO_BYTES.get(unit);
            if (bytesPerUnit == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Invalid unit: '" + unit + "'. Supported units: B, KB, MB, GB, TB, PB");
            }

            long sizeMb = (long) (value * bytesPerUnit / (1024 * 1024));
            if (sizeMb <= 0) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "target_size too small: " + targetSize + ", must be at least 1MB");
            }

            return sizeMb;
        } catch (NumberFormatException e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Failed to parse targetSize, reason: " + e.getMessage());
        }
    }
}
