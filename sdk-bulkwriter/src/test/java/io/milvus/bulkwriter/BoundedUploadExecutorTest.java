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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class BoundedUploadExecutorTest {
    @Test
    void doesNotReadAheadBeyondConcurrency() throws Exception {
        AtomicInteger read = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> {
                BoundedUploadExecutor.run(3, () -> {
                    int entry = read.incrementAndGet();
                    return entry <= 1000 ? entry : null;
                }, entry -> { started.countDown(); release.await(); });
                return null;
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            assertEquals(3, read.get());
            release.countDown();
            result.get(10, TimeUnit.SECONDS);
            assertEquals(1001, read.get());
        } finally { release.countDown(); caller.shutdownNow(); }
    }

    @Test
    void failedWorkerInterruptsPeersAndStopsReading() throws Exception {
        AtomicInteger read = new AtomicInteger();
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch peerStopped = new CountDownLatch(1);
        assertThrows(ExecutionException.class, () -> BoundedUploadExecutor.run(2,
                read::incrementAndGet, entry -> {
                    try {
                        bothStarted.countDown();
                        assertTrue(bothStarted.await(10, TimeUnit.SECONDS));
                        if (entry == 1) { throw new IOException("failed"); }
                        new CountDownLatch(1).await();
                    } finally {
                        // Cancellation can interrupt the initial barrier as well as the later wait.
                        if (entry == 2) { peerStopped.countDown(); }
                    }
                }));
        assertEquals(2, read.get());
        assertEquals(0, peerStopped.getCount());
    }
}
