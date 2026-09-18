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

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                "1 0 0 .",                       // RFC 2782 root target: service unavailable
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
    void resolveSrvParsesRecordsAndClosesContext() throws Exception {
        BasicAttributes attributes = new BasicAttributes();
        Attribute records = new BasicAttribute("SRV");
        records.add("2 0 8443 far.example.com.");
        records.add("1 0 19530 near.example.com.");
        records.add("1 0 0 .");
        attributes.put(records);
        DirContext context = dnsContext(attributes);
        doThrow(new NamingException("close failed")).when(context).close();
        AtomicReference<Hashtable<String, String>> environment = new AtomicReference<>();

        List<SrvTarget> targets = GlobalClusterUtils.resolveSrv(HOSTNAME, env -> {
            environment.set(env);
            return context;
        });

        assertEquals(2, targets.size());
        assertEquals("near.example.com", targets.get(0).target);
        assertEquals("far.example.com", targets.get(1).target);
        assertEquals("com.sun.jndi.dns.DnsContextFactory",
                environment.get().get(Context.INITIAL_CONTEXT_FACTORY));
        assertEquals("2000", environment.get().get("com.sun.jndi.dns.timeout.initial"));
        assertEquals("2", environment.get().get("com.sun.jndi.dns.timeout.retries"));
    }

    @Test
    void resolveSrvHandlesEmptyAnswersAndFailures() throws Exception {
        assertTrue(GlobalClusterUtils.resolveSrv(
                HOSTNAME, env -> dnsContext(new BasicAttributes())).isEmpty());

        BasicAttributes emptyAttribute = new BasicAttributes();
        emptyAttribute.put(new BasicAttribute("SRV"));
        assertTrue(GlobalClusterUtils.resolveSrv(
                HOSTNAME, env -> dnsContext(emptyAttribute)).isEmpty());

        DirContext notFound = mock(DirContext.class);
        when(notFound.getAttributes(anyString(), any(String[].class)))
                .thenThrow(new NameNotFoundException("nxdomain"));
        assertTrue(GlobalClusterUtils.resolveSrv(HOSTNAME, env -> notFound).isEmpty());

        DirContext failure = mock(DirContext.class);
        when(failure.getAttributes(anyString(), any(String[].class)))
                .thenThrow(new NamingException("dns unavailable"));
        RuntimeException queryFailure = assertThrows(RuntimeException.class,
                () -> GlobalClusterUtils.resolveSrv(HOSTNAME, env -> failure));
        assertTrue(queryFailure.getMessage().contains("dns unavailable"));

        RuntimeException initFailure = assertThrows(RuntimeException.class,
                () -> GlobalClusterUtils.resolveSrv(HOSTNAME, env -> {
                    throw new NamingException("init failed");
                }));
        assertTrue(initFailure.getMessage().contains("init failed"));
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
    void pickCandidatesTopUpHonorsWeightsWithoutReplacement() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "weightless.example.com"),
                new SrvTarget(1, 100, 19530, "heavy-a.example.com"),
                new SrvTarget(1, 100, 19530, "heavy-b.example.com"));
        Random zeroThreshold = new Random() {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        };

        List<SrvTarget> picked = GlobalClusterUtils.pickCandidates(targets, 2, zeroThreshold);

        assertEquals(Arrays.asList("heavy-a.example.com", "heavy-b.example.com"), Arrays.asList(
                picked.get(0).target, picked.get(1).target));
    }

    @Test
    void pickCandidatesDeduplicatesSameHostAndPortAcrossPriorities() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 100, 19530, "duplicate.example.com"),
                new SrvTarget(2, 100, 19530, "duplicate.example.com"),
                new SrvTarget(3, 100, 19530, "other.example.com"));

        List<SrvTarget> picked = GlobalClusterUtils.pickCandidates(targets, 2, new Random(42));

        assertEquals(2, picked.size());
        assertEquals("duplicate.example.com", picked.get(0).target);
        assertEquals("other.example.com", picked.get(1).target);

        SrvTarget first = targets.get(0);
        SrvTarget duplicate = targets.get(1);
        assertEquals(first, duplicate);
        assertEquals(first.hashCode(), duplicate.hashCode());
        assertFalse(first.equals(targets.get(2)));
        assertFalse(first.equals(new SrvTarget(1, 100, 19531, "duplicate.example.com")));
        assertFalse(first.equals("duplicate.example.com"));
        assertFalse(first.equals(null));
        assertTrue(first.toString().contains("duplicate.example.com:19530"));
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
                    HOSTNAME, 4L, null, h -> seeds.targets(), seeds.prober());
            assertEquals(5L, newer.getVersion());
            assertEquals("host-current:19530", newer.getPrimary().getEndpoint());

            // Nothing newer server-side: keep the cached topology.
            assertNull(GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, 5L, null, h -> seeds.targets(), seeds.prober()));
            assertNull(GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, 6L, null, h -> seeds.targets(), seeds.prober()));
        }
    }

    @Test
    void firstFetchReturnsFirstAnswerAndWatcherAppliesHigherVersion() throws Exception {
        // The nearest seed answers fast but lags; the remaining seed returns a higher version
        // only after a delay. The first answer must win immediately, and the background
        // watcher must trigger the replacement flow with the newer topology.
        CountDownLatch replaced = new CountDownLatch(1);
        AtomicReference<GlobalTopology> replacement = new AtomicReference<>();
        Predicate<GlobalTopology> watcher = topology -> {
            replacement.set(topology);
            replaced.countDown();
            return true;
        };
        try (SeedServers seeds = new SeedServers(
                () -> topologyJson(1, "host-lag:19530"),
                () -> {
                    sleepQuietly(300);
                    return topologyJson(2, "host-new:19530");
                })) {
            GlobalTopology first = GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, null, watcher, h -> seeds.targets(), seeds.prober());
            assertEquals(1L, first.getVersion());
            assertEquals("host-lag:19530", first.getPrimary().getEndpoint());

            assertTrue(replaced.await(5, TimeUnit.SECONDS), "watcher should apply the newer topology");
            assertEquals(2L, replacement.get().getVersion());
        }
    }

    @Test
    void cachedRefreshReturnsAfterGraceAndAppliesLateHigherVersion() throws Exception {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "fast"),
                new SrvTarget(1, 0, 19530, "slow"));
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch lateApplied = new CountDownLatch(1);
        AtomicReference<GlobalTopology> lateTopology = new AtomicReference<>();
        GlobalClusterUtils.SeedProber prober = target -> {
            if ("slow".equals(target.target)) {
                slowStarted.countDown();
                if (!releaseSlow.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting for test release");
                }
                return parse(topologyJson(6, "host-late:19530"));
            }
            return parse(topologyJson(5, "host-fast:19530"));
        };
        Predicate<GlobalTopology> watcher = topology -> {
            lateTopology.set(topology);
            lateApplied.countDown();
            return true;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<GlobalTopology> fetch = executor.submit(() -> GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, 4L, watcher, h -> targets, prober));
            assertTrue(slowStarted.await(2, TimeUnit.SECONDS));

            GlobalTopology foreground = fetch.get(3, TimeUnit.SECONDS);
            assertEquals(5L, foreground.getVersion());

            releaseSlow.countDown();
            assertTrue(lateApplied.await(3, TimeUnit.SECONDS));
            assertEquals(6L, lateTopology.get().getVersion());
        } finally {
            releaseSlow.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void cachedWatcherContinuesAfterCallbackRejectsStaleCandidate() throws Exception {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "fast"),
                new SrvTarget(1, 0, 19530, "rejected"),
                new SrvTarget(1, 0, 19530, "latest"));
        CountDownLatch releaseRejected = new CountDownLatch(1);
        CountDownLatch releaseLatest = new CountDownLatch(1);
        CountDownLatch rejectedSeen = new CountDownLatch(1);
        CountDownLatch latestApplied = new CountDownLatch(1);
        AtomicReference<GlobalTopology> latestTopology = new AtomicReference<>();
        GlobalClusterUtils.SeedProber prober = target -> {
            if ("rejected".equals(target.target)) {
                if (!releaseRejected.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting for rejected release");
                }
                return parse(topologyJson(6, "host-rejected:19530"));
            }
            if ("latest".equals(target.target)) {
                if (!releaseLatest.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting for latest release");
                }
                return parse(topologyJson(7, "host-latest:19530"));
            }
            return parse(topologyJson(5, "host-fast:19530"));
        };
        Predicate<GlobalTopology> watcher = topology -> {
            if (topology.getVersion() == 6) {
                rejectedSeen.countDown();
                return false;
            }
            if (topology.getVersion() == 7) {
                latestTopology.set(topology);
                latestApplied.countDown();
            }
            return true;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<GlobalTopology> fetch = executor.submit(() -> GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, 4L, watcher, h -> targets, prober));
            assertEquals(5L, fetch.get(3, TimeUnit.SECONDS).getVersion());

            releaseRejected.countDown();
            assertTrue(rejectedSeen.await(2, TimeUnit.SECONDS));
            releaseLatest.countDown();
            assertTrue(latestApplied.await(2, TimeUnit.SECONDS));
            assertEquals(7L, latestTopology.get().getVersion());
        } finally {
            releaseRejected.countDown();
            releaseLatest.countDown();
            executor.shutdownNow();
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
                    HOSTNAME, null, null, h -> seeds.targets(), prober);
            assertEquals(1L, first.getVersion());
            assertEquals(2, seeds.probeCount.get());

            // Refresh (cached version): every seed is probed and the highest version wins.
            seeds.probeCount.set(0);
            GlobalClusterUtils.fetchTopologyViaSrv(
                    HOSTNAME, 1L, null, h -> seeds.targets(), prober);
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
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null, null,
                        h -> Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1")), prober));
        assertEquals(401, exception.getCode());
        assertEquals(1, attempts.get(), "deterministic API errors must not be retried");
    }

    @Test
    void mixedApiAndTransientErrorsAreRetried() throws Exception {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "api"),
                new SrvTarget(1, 0, 19530, "down"));
        AtomicInteger apiAttempts = new AtomicInteger();
        AtomicInteger transientAttempts = new AtomicInteger();
        CountDownLatch transientFinished = new CountDownLatch(2);
        GlobalClusterUtils.SeedProber prober = target -> {
            if ("down".equals(target.target)) {
                transientAttempts.incrementAndGet();
                transientFinished.countDown();
                throw new IOException("connection refused");
            }
            if (apiAttempts.incrementAndGet() == 1) {
                throw new GlobalClusterApiException(7, "temporarily inconsistent seed");
            }
            return parse(topologyJson(4, "host-recovered:19530"));
        };

        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(
                HOSTNAME, null, null, h -> targets, prober);

        assertEquals(4L, topology.getVersion());
        assertEquals(2, apiAttempts.get());
        assertTrue(transientFinished.await(2, TimeUnit.SECONDS));
        assertEquals(2, transientAttempts.get());
    }

    @Test
    void authenticationErrorFailsFastEvenWithTransientFailure() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "auth"),
                new SrvTarget(1, 0, 19530, "down"));
        AtomicInteger authAttempts = new AtomicInteger();
        AtomicInteger transientAttempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            if ("down".equals(target.target)) {
                transientAttempts.incrementAndGet();
                throw new IOException("connection refused");
            }
            authAttempts.incrementAndGet();
            throw new GlobalClusterApiException(401, "unauthorized");
        };

        GlobalClusterApiException exception = assertThrows(GlobalClusterApiException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(
                        HOSTNAME, null, null, h -> targets, prober));

        assertEquals(401, exception.getCode());
        assertEquals(1, authAttempts.get());
        assertEquals(1, transientAttempts.get());
    }

    @Test
    void matchingApiErrorsFailFastWithoutRetry() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "api-a"),
                new SrvTarget(1, 0, 19530, "api-b"));
        AtomicInteger attempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            attempts.incrementAndGet();
            throw new GlobalClusterApiException(7, "same rejection");
        };

        GlobalClusterApiException exception = assertThrows(GlobalClusterApiException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(
                        HOSTNAME, null, null, h -> targets, prober));

        assertEquals(7, exception.getCode());
        assertEquals(2, attempts.get());
    }

    @Test
    void differentApiErrorsAreRetried() {
        List<SrvTarget> targets = Arrays.asList(
                new SrvTarget(1, 0, 19530, "api-a"),
                new SrvTarget(1, 0, 19530, "api-b"));
        AtomicInteger resolveAttempts = new AtomicInteger();
        GlobalClusterUtils.SeedProber prober = target -> {
            if (resolveAttempts.get() == 1) {
                throw new GlobalClusterApiException(
                        "api-a".equals(target.target) ? 7 : 8, "inconsistent rejection");
            }
            return parse(topologyJson(4, "host-recovered:19530"));
        };

        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null,
                null, h -> {
                    resolveAttempts.incrementAndGet();
                    return targets;
                }, prober);

        assertEquals(4L, topology.getVersion());
        assertEquals(2, resolveAttempts.get());
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

        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null,
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
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null, null,
                        h -> Collections.singletonList(new SrvTarget(1, 0, 19530, "seed1")), prober));
        assertTrue(exception.getMessage().contains("after 3 attempts"));
        assertEquals(3, attempts.get());
    }

    @Test
    void missingSrvRecordsFailFast() {
        AtomicInteger resolveAttempts = new AtomicInteger();
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null, null,
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
        GlobalTopology topology = GlobalClusterUtils.fetchTopologyViaSrv(HOSTNAME, null,
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

    private static DirContext dnsContext(BasicAttributes attributes) throws NamingException {
        DirContext context = mock(DirContext.class);
        when(context.getAttributes(anyString(), any(String[].class))).thenReturn(attributes);
        return context;
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
                add(() -> body);
            }
        }

        @SafeVarargs
        SeedServers(Supplier<String>... responders) throws IOException {
            for (Supplier<String> responder : responders) {
                add(responder);
            }
        }

        private void add(Supplier<String> responder) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/global-cluster/topology", exchange -> {
                String body = responder.get();
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
