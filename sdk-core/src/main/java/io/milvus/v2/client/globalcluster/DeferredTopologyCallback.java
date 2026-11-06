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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.v2.client.globalcluster;

import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

/**
 * Holds initial watcher callbacks until the owning connection has been fully published.
 * The gate deliberately has no reference to the owner until {@link #activate(Predicate)}.
 */
final class DeferredTopologyCallback implements Predicate<GlobalTopology> {
    private final CountDownLatch ready = new CountDownLatch(1);
    private volatile Predicate<GlobalTopology> target;
    private volatile boolean closed;

    @Override
    public boolean test(GlobalTopology topology) {
        Predicate<GlobalTopology> current = target;
        if (current == null) {
            try {
                ready.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            current = target;
        }
        return !closed && current != null && current.test(topology);
    }

    void activate(Predicate<GlobalTopology> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("topology callback is required");
        }
        if (!closed) {
            target = callback;
            ready.countDown();
        }
    }

    void close() {
        closed = true;
        ready.countDown();
    }
}
