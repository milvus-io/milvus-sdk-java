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

package io.milvus.v2.client.globalcluster;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * White-box tests for SRV-based topology discovery (same package to access package-private
 * discovery internals without reflection).
 */
@Tag("unit")
class SrvDiscoveryTest {

    private static final String HOSTNAME = "inst.global-cluster.example.com";

    @Test
    void endpointHostnameStripsSchemePortAndPath() {
        assertEquals("xxx.global-cluster.yyy.com",
                GlobalClusterUtils.endpointHostname(
                        "https://xxx.global-cluster.yyy.com:443/global-cluster/topology"));
        assertEquals("host.global-cluster.example",
                GlobalClusterUtils.endpointHostname("host.global-cluster.example"));
        assertEquals("host.global-cluster.example",
                GlobalClusterUtils.endpointHostname("  https://host.global-cluster.example:19530  "));
        assertEquals("host.global-cluster.example",
                GlobalClusterUtils.endpointHostname("https://user:pass@host.global-cluster.example/path"));

        assertThrows(RuntimeException.class, () -> GlobalClusterUtils.endpointHostname("https://"));
    }

    @Test
    void parseSrvRecordsParsesSortsAndSkipsInvalid() {
        List<SrvTarget> targets = GlobalClusterUtils.parseSrvRecords(new String[]{
                "2 30 8443 far.example.com.",
                "1 100 8443 near-b.example.com",
                "1 50 19530 near-a.example.com.",
                ". 0 0 unusable.example.com.",   // RFC 2782 root target: service unavailable
                "not numbers 8443 bad.example.com",
                "1 50"                            // malformed: too few fields
        });

        assertEquals(3, targets.size());
        // Nearest first: lower priority wins, higher weight breaks ties.
        assertEquals("near-b.example.com", targets.get(0).target);
        assertEquals(1, targets.get(0).priority);
        assertEquals(100, targets.get(0).weight);
        assertEquals(8443, targets.get(0).port);
        assertEquals("near-a.example.com", targets.get(1).target);
        assertEquals("far.example.com", targets.get(2).target);

        assertTrue(GlobalClusterUtils.parseSrvRecords(new String[0]).isEmpty());
        assertTrue(GlobalClusterUtils.parseSrvRecords(null).isEmpty());
    }

    @Test
    void pickCandidatesSpanPrioritiesThenTopUp() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 50, 19530, "a.example.com"),
                new SrvTarget(2, 50, 19530, "b.example.com"),
                new SrvTarget(3, 50, 19530, "c.example.com"));

        // One weighted pick per priority group, lowest priority first.
        List<SrvTarget> picked = GlobalClusterUtils.pickCandidates(targets, 2, new Random(42));
        assertEquals(2, picked.size());
        assertEquals(1, picked.get(0).priority);
        assertEquals(2, picked.get(1).priority);

        // Few priority groups: top up from the remaining nearest targets, no duplicates.
        List<SrvTarget> single = Arrays.asList(
                new SrvTarget(1, 0, 19530, "a.example.com"),
                new SrvTarget(1, 0, 19530, "b.example.com"));
        List<SrvTarget> pickedFromSingle = GlobalClusterUtils.pickCandidates(single, 2, new Random(42));
        assertEquals(2, pickedFromSingle.size());
        assertTrue(pickedFromSingle.get(0).target.equals("a.example.com")
                ? pickedFromSingle.get(1).target.equals("b.example.com")
                : pickedFromSingle.get(1).target.equals("a.example.com"));

        assertTrue(GlobalClusterUtils.pickCandidates(targets, 0).isEmpty());
        assertTrue(GlobalClusterUtils.pickCandidates(new ArrayList<>(), 2).isEmpty());
    }

    @Test
    void pickCandidatesHonorSrvWeights() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 100, 19530, "heavy.example.com"),
                new SrvTarget(1, 0, 19530, "weightless.example.com"));
        Random random = new Random(42);

        for (int i = 0; i < 20; i++) {
            List<SrvTarget> picked = GlobalClusterUtils.pickCandidates(targets, 1, random);
            assertEquals("heavy.example.com", picked.get(0).target);
        }
    }

    @Test
    void buildSeedTopologyUrlPassesHostnameVerbatim() {
        SrvTarget target = new SrvTarget(1, 100, 8443, "seed1.example.com");
        assertEquals("https://seed1.example.com:8443/global-cluster/topology?endpoint=" + HOSTNAME,
                GlobalClusterUtils.buildSeedTopologyUrl(target, HOSTNAME));
    }

    @Test
    void cachedVersionGatesRefreshAgainstRegression() throws Exception {
        // Two seeds; the nearest lags behind (partial replication) while the second is current.
        try (SeedServers seeds = new SeedServers(
                topologyJson(3, "host-lag:19530"),
                topologyJson(5, "host-current:19530"))) {

            GlobalTopology newer = GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", 4L, null, h -> seeds.targets(), seeds.prober());
            assertEquals(5L, newer.getVersion());
            assertEquals("host-current:19530", newer.getPrimary().getEndpoint());

            // Nothing newer server-side: keep the cached topology.
            assertNull(GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", 5L, null, h -> seeds.targets(), seeds.prober()));
            assertNull(GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", 6L, null, h -> seeds.targets(), seeds.prober()));
        }
    }

    @Test
    void firstFetchReturnsFirstAnswerAndWatcherAppliesHigherVersion() throws Exception {
        // The nearest seed answers fast but lags; the remaining seed returns a higher version
        // only after a delay. The first answer must win immediately, and the background
        // watcher must trigger the replacement flow with the newer topology.
        CountDownLatch replaced = new CountDownLatch(1);
        AtomicReference<GlobalTopology> replacement = new AtomicReference<>();
        Consumer<GlobalTopology> watcher = topology -> {
            replacement.set(topology);
            replaced.countDown();
        };
        try (SeedServers seeds = new SeedServers(
                index -> topologyJson(1, "host-lag:19530"),
                index -> {
                    sleepQuietly(300);
                    return topologyJson(2, "host-new:19530");
                })) {
            GlobalTopology first = GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", null, watcher, h -> seeds.targets(), seeds.prober());
            assertEquals(1L, first.getVersion());
            assertEquals("host-lag:19530", first.getPrimary().getEndpoint());

            assertTrue(replaced.await(5, TimeUnit.SECONDS), "watcher should apply the newer topology");
            assertEquals(2L, replacement.get().getVersion());
        }
    }

    @Test
    void firstFetchProbesNearestSeedsOnlyRefreshProbesAll() throws Exception {
        try (SeedServers seeds = new SeedServers(
                topologyJson(1, "host1:19530"),
                topologyJson(1, "host2:19530"),
                topologyJson(1, "host3:19530"))) {
            GlobalClusterUtils.SeedProber prober = seeds.prober();

            // First fetch (no cached version): only DEFAULT_PROBE_COUNT seeds are probed.
            GlobalTopology first = GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", null, null, h -> seeds.targets(), prober);
            assertEquals(1L, first.getVersion());
            assertEquals(2, seeds.probeCount.get());

            // Refresh (cached version): every seed is probed and the highest version wins.
            seeds.probeCount.set(0);
            GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, "token", 1L, null, h -> seeds.targets(), prober);
            assertEquals(3, seeds.probeCount.get());
        }
    }

    @Test
    void apiErrorFailsFastWithoutRetry() {
        AtomicInteger attempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            attempts.incrementAndGet();
            throw new GlobalClusterApiException(401, "unauthorized");
        };

        GlobalClusterApiException exception = assertThrows(GlobalClusterApiException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, "token", null, null,
                        h -> Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1")), prober));
        assertEquals(401, exception.getCode());
        assertEquals(1, attempts.get(), "deterministic API errors must not be retried");
    }

    @Test
    void unreachableSeedsRetryThenSucceed() {
        AtomicInteger attempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("connection refused");
            }
            return parse(topologyJson(4, "host-recovered:19530"));
        };

        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, "token", null,
                null, h -> Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1")), prober);
        assertEquals(4L, topology.getVersion());
        assertEquals("host-recovered:19530", topology.getPrimary().getEndpoint());
        assertEquals(2, attempts.get(), "transient seed failures must be retried");
    }

    @Test
    void unreachableSeedsExhaustRetries() {
        AtomicInteger attempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            attempts.incrementAndGet();
            throw new IOException("connection refused");
        };

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, "token", null, null,
                        h -> Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1")), prober));
        assertTrue(exception.getMessage().contains("after 3 attempts"));
        assertEquals(3, attempts.get());
    }

    @Test
    void missingSrvRecordsFailFast() {
        AtomicInteger resolveAttempts = new AtomicInteger();
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, "token", null, null,
                        h -> {
                            resolveAttempts.incrementAndGet();
                            return Collections.emptyList();
                        },
                        target -> parse(topologyJson(1, "host1:19530"))));
        assertTrue(exception.getMessage().contains("No SRV records"));
        assertEquals(1, resolveAttempts.get(), "a missing SRV record set is deterministic: no retry");
    }

    @Test
    void transientDnsFailuresAreRetried() {
        AtomicInteger resolveAttempts = new AtomicInteger();
        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, "token", null,
                null, h -> {
                    if (resolveAttempts.incrementAndGet() == 1) {
                        throw new RuntimeException("DNS timeout");
                    }
                    return Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1"));
                },
                target -> parse(topologyJson(2, "host1:19530")));
        assertEquals(2L, topology.getVersion());
        assertEquals(2, resolveAttempts.get());
    }

    private static String topologyJson(long version, String endpoint) {
        return "{\"code\":0,\"data\":{\"version\":" + version + ",\"clusters\":["
                + "{\"clusterId\":\"c1\",\"endpoint\":\"" + endpoint + "\",\"capability\":3}]}}";
    }

    private static GlobalTopology parse(String json) {
        return GlobalClusterUtils.parseTopologyResponse(json);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Spins up local HTTP servers that stand in for ha-manager seeds: one per supplied
     * responder, listening on 127.0.0.1 with the SrvTarget port set to the server port.
     */
    private static final class SeedServers implements AutoCloseable {
        private final List<HttpServer> servers = new ArrayList<>();
        final AtomicInteger probeCount = new AtomicInteger();

        SeedServers(String... bodies) throws IOException {
            for (String body : bodies) {
                add(index -> body);
            }
        }

        @SafeVarargs
        SeedServers(IntFunction<String>... responders) throws IOException {
            for (IntFunction<String> responder : responders) {
                add(responder);
            }
        }

        private void add(IntFunction<String> responder) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/global-cluster/topology", exchange -> {
                String body = responder.apply(0);
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });
            server.start();
            servers.add(server);
        }

        List<SrvTarget> targets() {
            List<SrvTarget> targets = new ArrayList<>();
            for (HttpServer server : servers) {
                targets.add(new SrvTarget(1, 0, server.getAddress().getPort(), "127.0.0.1"));
            }
            return targets;
        }

        GlobalClusterUtils.SeedProber prober() {
            return target -> {
                probeCount.incrementAndGet();
                String body = GlobalClusterUtils.doHttpGet(
                        "http://127.0.0.1:" + target.port + "/global-cluster/topology?endpoint="
                                + HOSTNAME, "token");
                return parse(body);
            };
        }

        @Override
        public void close() {
            for (HttpServer server : servers) {
                server.stop(0);
            }
        }
    }
}
