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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DeferredTopologyCallbackTest {

    @Test
    void interruptWhileWaitingReturnsFalse() throws Exception {
        DeferredTopologyCallback gate = new DeferredTopologyCallback();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Boolean> result = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            started.countDown();
            result.set(gate.test(topology(1)));
            finished.countDown();
        });
        caller.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));

        caller.interrupt();

        assertTrue(finished.await(2, TimeUnit.SECONDS));
        assertFalse(result.get());
        gate.close();
    }

    @Test
    void closePreventsLaterActivation() {
        DeferredTopologyCallback gate = new DeferredTopologyCallback();
        gate.close();

        gate.activate(topology -> true);

        assertFalse(gate.test(topology(1)));
    }

    @Test
    void nullActivationCallbackIsRejected() {
        DeferredTopologyCallback gate = new DeferredTopologyCallback();

        assertThrows(IllegalArgumentException.class, () -> gate.activate(null));
        gate.close();
    }

    private static GlobalTopology topology(long version) {
        return new GlobalTopology(version, Collections.singletonList(
                new ClusterInfo("cluster", "host:19530", ClusterCapability.WRITABLE)));
    }
}
