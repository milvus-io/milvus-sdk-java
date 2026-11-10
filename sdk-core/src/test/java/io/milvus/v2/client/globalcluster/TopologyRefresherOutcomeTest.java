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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TopologyRefresherOutcomeTest {

    @Test
    void staleCandidateIsLoggedAsRejectedInsteadOfReconnecting() throws Exception {
        GlobalTopology current = topology(1);
        GlobalTopology stale = topology(2);
        TopologyFetcher fetcher = (endpoint, token, cachedVersion, callback) -> stale;
        TopologyRefresher refresher = new TopologyRefresher(
                "https://host.global-cluster.example", "token", () -> current,
                topology -> false, fetcher);
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
            refresher.refresh();
        } finally {
            System.setErr(originalErr);
            refresher.stop();
        }

        String logs = captured.toString(StandardCharsets.UTF_8.name());
        assertTrue(logs.contains("rejected as stale"), logs);
        assertFalse(logs.contains("triggering reconnection"), logs);
    }

    @Test
    void acceptedCandidateUsesCurrentVersionAndInstallsUpdate() {
        GlobalTopology current = topology(1);
        GlobalTopology newer = topology(2);
        AtomicReference<GlobalTopology> applied = new AtomicReference<>();
        TopologyFetcher fetcher = (endpoint, token, cachedVersion, callback) -> {
            assertEquals(1L, cachedVersion);
            return newer;
        };
        TopologyRefresher refresher = new TopologyRefresher(
                "https://host.global-cluster.example", "token", () -> current,
                topology -> {
                    applied.set(topology);
                    return true;
                }, fetcher);
        try {
            refresher.refresh();
            assertSame(newer, applied.get());
        } finally {
            refresher.stop();
        }
    }

    @Test
    void nullAndFailedFetchesKeepCurrentTopology() {
        GlobalTopology current = topology(1);
        TopologyRefresher noUpdate = new TopologyRefresher(
                "https://host.global-cluster.example", "token", () -> current,
                topology -> {
                    throw new AssertionError("no update should not invoke the callback");
                }, (endpoint, token, cachedVersion, callback) -> null);
        TopologyRefresher failure = new TopologyRefresher(
                "https://host.global-cluster.example", "token", () -> current,
                topology -> true, (endpoint, token, cachedVersion, callback) -> {
                    throw new IllegalStateException("DNS unavailable");
                });
        try {
            noUpdate.refresh();
            failure.refresh();
        } finally {
            noUpdate.stop();
            failure.stop();
        }
    }

    @Test
    void triggeredRefreshRunsAndCleansUp() throws Exception {
        GlobalTopology current = topology(1);
        GlobalTopology newer = topology(2);
        CountDownLatch refreshed = new CountDownLatch(1);
        TopologyRefresher refresher = new TopologyRefresher(
                "https://host.global-cluster.example", "token", () -> current,
                topology -> {
                    refreshed.countDown();
                    return true;
                }, (endpoint, token, cachedVersion, callback) -> newer);
        try {
            refresher.triggerRefresh();
            assertTrue(refreshed.await(2, TimeUnit.SECONDS));
        } finally {
            refresher.stop();
        }
    }

    private static GlobalTopology topology(long version) {
        return new GlobalTopology(version, Collections.singletonList(
                new ClusterInfo("cluster", "host:19530", ClusterCapability.WRITABLE)));
    }
}
