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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/** Streams work with at most concurrency entries/futures retained, including queued work. */
final class BoundedUploadExecutor {
    @FunctionalInterface
    interface Source<T> { T next() throws Exception; }

    @FunctionalInterface
    interface Action<T> { void run(T value) throws Exception; }

    static <T> void run(int concurrency, Source<T> source, Action<T> action) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CompletionService<Void> completed = new ExecutorCompletionService<>(executor);
        Set<Future<Void>> active = new HashSet<>();
        try {
            boolean exhausted = false;
            while (!exhausted || !active.isEmpty()) {
                while (!exhausted && active.size() < concurrency) {
                    UploadManifest.checkInterrupted();
                    T entry = source.next();
                    if (entry == null) {
                        exhausted = true;
                    } else {
                        active.add(completed.submit(() -> { action.run(entry); return null; }));
                    }
                }
                if (!active.isEmpty()) {
                    Future<Void> finished = completed.take();
                    active.remove(finished);
                    finished.get();
                }
            }
        } finally {
            for (Future<Void> future : active) { future.cancel(true); }
            executor.shutdownNow();
            // Do not close clients or delete the manifest while a worker still uses them.
            boolean interrupted = Thread.interrupted();
            while (!executor.isTerminated()) {
                try { executor.awaitTermination(1, TimeUnit.SECONDS); }
                catch (InterruptedException e) { interrupted = true; }
            }
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }
}
