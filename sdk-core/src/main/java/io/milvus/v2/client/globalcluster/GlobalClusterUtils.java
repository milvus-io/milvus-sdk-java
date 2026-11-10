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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Zilliz Cloud only. Utility methods for detecting global cluster endpoints and discovering the
 * global cluster topology via DNS SRV records.
 * <p>
 * The SDK never talks to the global endpoint directly. It derives an SRV query name from the
 * endpoint hostname ({@code _grpc._tcp.<hostname>}), resolves it to a set of ha-manager seed
 * servers, and fetches the topology from them. The global cluster id is not parsed client-side:
 * the endpoint hostname is passed verbatim to the server via the {@code ?endpoint=} query
 * parameter, and the server derives the id from it (its first DNS label).
 *
 * @zillizCloudOnly
 */

public class GlobalClusterUtils {
    private static final Logger logger = LoggerFactory.getLogger(GlobalClusterUtils.class);

    private static final String GLOBAL_CLUSTER_MARKER = "global-cluster";
    private static final String TOPOLOGY_PATH = "/global-cluster/topology";
    private static final int MAX_RETRIES = 3;
    private static final long BASE_BACKOFF_MS = 1000;
    private static final long MAX_BACKOFF_MS = 10000;
    private static final int REQUEST_TIMEOUT_MS = 10000;
    private static final long PROBE_GRACE_PERIOD_MS = 1000;

    private static final String SRV_SERVICE_PREFIX = "_grpc._tcp.";
    private static final String ENDPOINT_QUERY_PARAM = "endpoint";
    // Number of seeds probed concurrently on the first fetch (nothing cached yet), spanning
    // priorities so a slow or dead nearest region does not stall discovery. Refresh paths
    // (a version is already cached) probe every seed instead.
    private static final int DEFAULT_PROBE_COUNT = 2;
    private static final String DNS_CONTEXT_FACTORY = "com.sun.jndi.dns.DnsContextFactory";
    private static final String DNS_QUERY_TIMEOUT_MS = "2000";
    private static final String DNS_QUERY_RETRIES = "2";

    private GlobalClusterUtils() {
    }

    /**
     * Returns whether the given URI points to a global cluster endpoint.
     *
     * @param uri the connection URI
     * @return true if the URI is a global cluster endpoint
     */
    public static boolean isGlobalEndpoint(String uri) {
        if (uri == null) {
            return false;
        }
        return uri.toLowerCase().contains(GLOBAL_CLUSTER_MARKER);
    }

    /**
     * Fetches the global cluster topology via SRV-based seed discovery (no cached version).
     *
     * @param globalEndpoint the global cluster endpoint
     * @param token the authentication token, or null
     * @return the fetched topology
     * @throws RuntimeException if the topology cannot be fetched after all retries
     */
    public static GlobalTopology fetchTopology(String globalEndpoint, String token) {
        return fetchTopology(globalEndpoint, token, null, null);
    }

    /**
     * Fetches the global cluster topology via SRV-based seed discovery.
     * <p>
     * Resolves {@code _grpc._tcp.<hostname>} to ha-manager seeds, then concurrently probes
     * them and returns the highest foreground topology version.
     *
     * @param globalEndpoint the global cluster endpoint
     * @param token the authentication token, or null
     * @param cachedVersion version of the topology already cached locally, if any. When set,
     *                      every seed is probed and the first success starts a grace deadline;
     *                      when null, the first successful answer is returned immediately
     * @param onTopologyChange replacement callback invoked for later answers whose version
     *                         exceeds the foreground best, on both initial and cached fetches
     * @return the fetched topology, or null when {@code cachedVersion} is set and no seed
     *         reports a strictly higher version (the cached topology is already up to date)
     * @throws GlobalClusterApiException if the server rejects the request (e.g. auth failure)
     * @throws RuntimeException if the endpoint has no SRV records, or DNS / seed probing fails
     *                          after all retries
     */
    public static GlobalTopology fetchTopology(String globalEndpoint, String token,
                                               Long cachedVersion,
                                               Predicate<GlobalTopology> onTopologyChange) {
        String hostname = endpointHostname(globalEndpoint);
        return fetchTopologyViaSrv(hostname, cachedVersion, onTopologyChange,
                GlobalClusterUtils::resolveSrv, target -> probeSeed(target, hostname, token));
    }

    /**
     * Discovery loop over a hostname: SRV resolution and seed probing both run inside the
     * retry loop because DNS timeouts and unreachable seeds are transient and deserve the same
     * backoff. Deterministic states (no SRV records, authentication failure, consistent
     * server-side API rejection, a cached version no seed exceeds) fail fast without retry.
     */
    static GlobalTopology fetchTopologyViaSrv(String hostname, Long cachedVersion,
                                              Predicate<GlobalTopology> onTopologyChange,
                                              SrvResolver resolver, SeedProber prober) {
        Exception lastError = null;
        int seedCount = 0;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            List<SrvTarget> targets;
            try {
                targets = resolver.resolve(hostname);
            } catch (RuntimeException e) {
                lastError = e;
                logger.warn("SRV resolution failed for '{}' (attempt {}/{}): {}",
                        hostname, attempt, MAX_RETRIES, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    sleepBackoff(attempt);
                }
                continue;
            }
            if (targets.isEmpty()) {
                // NXDOMAIN / empty answer is a deterministic "not provisioned" state: fail
                // fast instead of falling back to a direct-endpoint fetch.
                throw new RuntimeException("No SRV records found for global cluster endpoint '"
                        + hostname + "' (" + SRV_SERVICE_PREFIX + hostname + ")");
            }
            seedCount = targets.size();
            // First fetch: probe the nearest DEFAULT_PROBE_COUNT seeds so a slow region does
            // not stall the initial connection. With a cached version the cache must not
            // regress, so probe every seed and take the highest version — partial
            // replication means the nearest seeds may lag.
            boolean cachedRefresh = cachedVersion != null;
            List<SrvTarget> candidates = pickCandidates(targets,
                    cachedRefresh ? targets.size() : DEFAULT_PROBE_COUNT);
            ProbeOutcome outcome = probeCandidates(candidates, prober, cachedRefresh, onTopologyChange);
            if (outcome.best != null) {
                if (!cachedRefresh || outcome.best.getVersion() > cachedVersion) {
                    return outcome.best;
                }
                // Every reachable seed is at or behind the cached version: nothing newer
                // on the server side, keep the cached topology.
                return null;
            }
            if (outcome.authenticationError != null) {
                // Credentials are shared by every seed; retrying them cannot help.
                throw outcome.authenticationError;
            }
            if (outcome.apiError != null && outcome.lastError == null && outcome.apiErrorsAgree) {
                // Every reachable seed returned the same API-level rejection.
                throw outcome.apiError;
            }
            lastError = outcome.lastError != null ? outcome.lastError : outcome.apiError;
            logger.warn("No usable topology response from {} seed(s) for '{}' (attempt {}/{}): {}",
                    candidates.size(), hostname, attempt, MAX_RETRIES,
                    lastError == null ? "unknown error" : lastError.getMessage());
            if (attempt < MAX_RETRIES) {
                sleepBackoff(attempt);
            }
        }
        String message = "Failed to fetch global topology from " + seedCount + " seed(s) for '"
                + hostname + "' after " + MAX_RETRIES + " attempts";
        throw new RuntimeException(lastError == null ? message : message + "; last error: "
                + lastError.getMessage(), lastError);
    }

    /**
     * Extracts the bare hostname from a global cluster endpoint. Strips scheme, credentials,
     * port and path so the result can be used both to derive the SRV query name and as the
     * verbatim {@code ?endpoint=} value.
     */
    static String endpointHostname(String globalEndpoint) {
        String endpoint = globalEndpoint.trim();
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }
        try {
            String hostname = new URL(endpoint).getHost();
            if (hostname == null || hostname.isEmpty()) {
                throw new RuntimeException("Invalid global cluster endpoint: " + globalEndpoint);
            }
            return hostname;
        } catch (IOException e) {
            throw new RuntimeException("Invalid global cluster endpoint: " + globalEndpoint, e);
        }
    }

    /**
     * Resolves the {@code _grpc._tcp.<hostname>} SRV record set via the JDK's JNDI DNS provider
     * using the system DNS servers.
     * <p>
     * Returns an empty list when the name does not exist or carries no SRV records (a
     * deterministic "not provisioned" state) so the caller can raise a clear error. Transient
     * DNS failures (timeout, no reachable nameserver) are raised as {@link RuntimeException}.
     */
    static List<SrvTarget> resolveSrv(String hostname) {
        return resolveSrv(hostname, InitialDirContext::new);
    }

    static List<SrvTarget> resolveSrv(String hostname, DnsContextFactory contextFactory) {
        String srvName = SRV_SERVICE_PREFIX + hostname;
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, DNS_CONTEXT_FACTORY);
        env.put("com.sun.jndi.dns.timeout.initial", DNS_QUERY_TIMEOUT_MS);
        env.put("com.sun.jndi.dns.timeout.retries", DNS_QUERY_RETRIES);
        DirContext ctx;
        try {
            ctx = contextFactory.create(env);
        } catch (NamingException e) {
            throw new RuntimeException("Failed to initialize DNS context for SRV resolution: "
                    + e.getMessage(), e);
        }
        try {
            Attributes attrs = ctx.getAttributes(srvName, new String[]{"SRV"});
            Attribute attr = attrs.get("SRV");
            if (attr == null || attr.size() == 0) {
                return Collections.emptyList();
            }
            String[] records = new String[attr.size()];
            for (int i = 0; i < records.length; i++) {
                records[i] = (String) attr.get(i);
            }
            return parseSrvRecords(records);
        } catch (NameNotFoundException e) {
            // NXDOMAIN: deterministic "not provisioned" state, not transient.
            return Collections.emptyList();
        } catch (NamingException e) {
            throw new RuntimeException("SRV resolution failed for " + srvName + ": "
                    + e.getMessage(), e);
        } finally {
            try {
                ctx.close();
            } catch (NamingException e) {
                logger.debug("Failed to close DNS context: {}", e.getMessage());
            }
        }
    }

    /**
     * Parses raw SRV record strings, each in {@code "priority weight port target"} form, into
     * targets sorted nearest first (lower priority wins, higher weight breaks ties).
     * <p>
     * Per RFC 2782 a target of "." means the service is decidedly not available; such records
     * (and malformed ones) are skipped.
     */
    static List<SrvTarget> parseSrvRecords(String[] records) {
        List<SrvTarget> targets = new ArrayList<>();
        if (records == null) {
            return targets;
        }
        for (String record : records) {
            if (record == null || record.trim().isEmpty()) {
                continue;
            }
            String[] parts = record.trim().split("\\s+");
            if (parts.length != 4) {
                logger.warn("Skipping malformed SRV record: {}", record);
                continue;
            }
            try {
                int priority = Integer.parseInt(parts[0]);
                int weight = Integer.parseInt(parts[1]);
                int port = Integer.parseInt(parts[2]);
                String target = parts[3];
                if (target.endsWith(".")) {
                    target = target.substring(0, target.length() - 1);
                }
                if (target.isEmpty()) {
                    continue;
                }
                targets.add(new SrvTarget(priority, weight, port, target));
            } catch (NumberFormatException e) {
                logger.warn("Skipping malformed SRV record: {}", record);
            }
        }
        Collections.sort(targets, (a, b) -> {
            int byPriority = Integer.compare(a.priority, b.priority);
            return byPriority != 0 ? byPriority : Integer.compare(b.weight, a.weight);
        });
        return targets;
    }

    /**
     * Selects seeds to probe: one weighted pick per priority group, lowest (nearest) priority
     * first, then topped up with weighted picks without replacement from all remaining targets.
     * Spans priorities so a dead nearest region does not stall discovery, and returns at least
     * {@code count} seeds when that many exist.
     */
    static List<SrvTarget> pickCandidates(List<SrvTarget> targets, int count) {
        return pickCandidates(targets, count, ThreadLocalRandom.current());
    }

    static List<SrvTarget> pickCandidates(List<SrvTarget> targets, int count, Random random) {
        List<SrvTarget> picked = new ArrayList<>();
        if (targets.isEmpty() || count <= 0) {
            return picked;
        }
        List<SrvTarget> remaining = new ArrayList<>();
        for (SrvTarget target : targets) {
            if (!remaining.contains(target)) {
                remaining.add(target);
            }
        }
        Map<Integer, List<SrvTarget>> byPriority = new TreeMap<>();
        for (SrvTarget target : remaining) {
            byPriority.computeIfAbsent(target.priority, k -> new ArrayList<>()).add(target);
        }
        for (List<SrvTarget> group : byPriority.values()) {
            SrvTarget choice = weightedChoice(group, random);
            picked.add(choice);
            remaining.remove(choice);
            if (picked.size() >= count) {
                return picked;
            }
        }
        while (picked.size() < count && !remaining.isEmpty()) {
            SrvTarget choice = weightedChoice(remaining, random);
            picked.add(choice);
            remaining.remove(choice);
        }
        return picked;
    }

    private static SrvTarget weightedChoice(List<SrvTarget> group, Random random) {
        long total = 0;
        for (SrvTarget target : group) {
            total += Math.max(target.weight, 0);
        }
        if (total == 0) {
            // All-zero weights: pick uniformly at random.
            return group.get(random.nextInt(group.size()));
        }
        long threshold = (long) (random.nextDouble() * total);
        for (SrvTarget target : group) {
            threshold -= Math.max(target.weight, 0);
            if (threshold < 0) {
                return target;
            }
        }
        return group.get(group.size() - 1);
    }

    static String buildSeedTopologyUrl(SrvTarget target, String hostname) {
        return "https://" + target.target + ":" + target.port + TOPOLOGY_PATH
                + "?" + ENDPOINT_QUERY_PARAM + "=" + hostname;
    }

    /**
     * Fetches topology from a single seed. Raises on any failure: {@link IOException} for
     * transport/HTTP errors (retryable), {@link GlobalClusterApiException} for deterministic
     * server-side rejections. The global cluster is identified by the raw hostname, passed
     * verbatim via the {@code ?endpoint=} query parameter.
     */
    static GlobalTopology probeSeed(SrvTarget target, String hostname, String token)
            throws IOException {
        return parseTopologyResponse(doHttpGet(buildSeedTopologyUrl(target, hostname), token));
    }

    /**
     * Probes seeds concurrently and classifies the results.
     * <p>
     * With {@code cachedRefresh} (a topology version is already cached locally) every seed is probed
     * and the highest foreground version wins. The first success starts a grace deadline so an
     * unavailable seed cannot stall recovery. Without it (first fetch) the first successful
     * answer wins. In both modes, remaining answers are drained in the background and
     * {@code onHigherVersion} triggers the replacement flow for each newer version.
     * API-level errors are tracked separately from transport failures so the caller can
     * distinguish a consistent server-side rejection from mixed or transient seed failures.
     * <p>
     * Probes run in plain daemon threads (not an {@link java.util.concurrent.ExecutorService},
     * whose non-daemon workers are joined at JVM exit): once the caller has its answer,
     * leftover probes must never delay process shutdown.
     */
    static ProbeOutcome probeCandidates(List<SrvTarget> candidates, SeedProber prober,
                                        boolean cachedRefresh,
                                        Predicate<GlobalTopology> onHigherVersion) {
        GlobalTopology best = null;
        GlobalClusterApiException apiError = null;
        GlobalClusterApiException authenticationError = null;
        boolean apiErrorsAgree = true;
        Exception lastError = null;
        LinkedBlockingQueue<ProbeResult> results = new LinkedBlockingQueue<>();
        int index = 0;
        for (SrvTarget candidate : candidates) {
            SrvTarget target = candidate;
            int id = index++;
            Thread probe = new Thread(() -> {
                try {
                    results.add(new ProbeResult(target, prober.probe(target), null));
                } catch (Exception e) {
                    results.add(new ProbeResult(target, null, e));
                }
            }, "milvus-global-topology-probe-" + id);
            probe.setDaemon(true);
            probe.start();
        }

        int pending = candidates.size();
        long deadlineNanos = 0L;
        while (pending > 0) {
            ProbeResult result;
            try {
                if (deadlineNanos == 0L) {
                    result = results.take();
                } else {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        break;
                    }
                    result = results.poll(remainingNanos, TimeUnit.NANOSECONDS);
                    if (result == null) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lastError = e;
                break;
            }
            pending--;
            if (result.error != null) {
                if (result.error instanceof GlobalClusterApiException) {
                    GlobalClusterApiException currentError =
                            (GlobalClusterApiException) result.error;
                    if (apiError == null) {
                        apiError = currentError;
                    } else if (apiError.getCode() != currentError.getCode()) {
                        apiErrorsAgree = false;
                    }
                    if ((currentError.getCode() == HttpURLConnection.HTTP_UNAUTHORIZED
                            || currentError.getCode() == HttpURLConnection.HTTP_FORBIDDEN)
                            && authenticationError == null) {
                        authenticationError = currentError;
                    }
                } else {
                    lastError = result.error;
                    logger.warn("Topology probe failed for {}: {}", result.target,
                            result.error.getMessage());
                }
                continue;
            }
            if (best == null || result.topology.getVersion() > best.getVersion()) {
                best = result.topology;
            }
            if (cachedRefresh) {
                if (deadlineNanos == 0L) {
                    // Once something usable arrives, bound the wait for lagging or unavailable
                    // seeds. Late answers continue through the background watcher below.
                    deadlineNanos = System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(PROBE_GRACE_PERIOD_MS);
                }
            } else {
                break; // First successful answer wins; the watcher keeps an eye on the rest.
            }
        }

        if (best != null && onHigherVersion != null && pending > 0) {
            watchRemaining(results, pending, best, onHigherVersion);
        }
        return new ProbeOutcome(best, apiError, authenticationError, apiErrorsAgree, lastError);
    }

    /**
     * Drains the answers still in flight and triggers replacement on a newer one.
     * <p>
     * Runs in a daemon thread after the first answer is handed over, so a slow seed does not
     * stall the initial connection while a newer version from the remaining seeds can still
     * win the replacement flow. Only results the caller did not consume arrive here, so each
     * seed is logged at most once.
     */
    private static void watchRemaining(LinkedBlockingQueue<ProbeResult> results, int remaining,
                                       GlobalTopology baseline,
                                       Predicate<GlobalTopology> onHigherVersion) {
        Thread watcher = new Thread(() -> {
            long bestVersion = baseline.getVersion();
            try {
                for (int i = 0; i < remaining; i++) {
                    ProbeResult result = results.take();
                    if (result.error != null) {
                        // One bad seed must not fail the rest.
                        logger.warn("Topology probe failed for {}: {}", result.target,
                                result.error.getMessage());
                        continue;
                    }
                    if (result.topology.getVersion() > bestVersion) {
                        bestVersion = result.topology.getVersion();
                        try {
                            boolean accepted = onHigherVersion.test(result.topology);
                            if (!accepted) {
                                logger.debug("Global cluster: topology version {} rejected as stale",
                                        result.topology.getVersion());
                            }
                        } catch (RuntimeException e) {
                            logger.warn("Topology replacement callback failed", e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "milvus-global-topology-watcher");
        watcher.setDaemon(true);
        watcher.start();
    }

    static String doHttpGet(String urlStr, String token) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(REQUEST_TIMEOUT_MS);
            conn.setReadTimeout(REQUEST_TIMEOUT_MS);
            conn.setRequestProperty("Accept", "application/json");
            if (token != null && !token.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + token);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED
                        || responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
                    String detail;
                    try {
                        detail = readBody(conn.getErrorStream());
                    } catch (IOException e) {
                        detail = "";
                    }
                    throw new GlobalClusterApiException(responseCode,
                            "Global topology API authentication failed with HTTP status "
                                    + responseCode + (detail.isEmpty() ? "" : ": " + detail));
                }
                throw new IOException("HTTP request failed with status code: " + responseCode);
            }

            return readBody(conn.getInputStream());
        } finally {
            conn.disconnect();
        }
    }

    private static String readBody(InputStream input) throws IOException {
        if (input == null) {
            return "";
        }
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }
        return response.toString();
    }

    /**
     * Parses a global topology API response into a {@link GlobalTopology}.
     *
     * @param responseBody the raw JSON response body
     * @return the parsed topology
     * @throws GlobalClusterApiException if the API returns a deterministic error code
     * @throws RuntimeException if the payload is malformed
     */
    static GlobalTopology parseTopologyResponse(String responseBody) {
        JsonObject root = JsonParser.parseString(responseBody).getAsJsonObject();
        int code = root.get("code").getAsInt();
        if (code != 0) {
            String message = root.has("message") ? root.get("message").getAsString() : "unknown error";
            throw new GlobalClusterApiException(code, "Global topology API returned error code "
                    + code + ": " + message);
        }

        JsonObject data = root.getAsJsonObject("data");
        long version = data.get("version").getAsLong();
        JsonArray clustersArray = data.getAsJsonArray("clusters");

        List<ClusterInfo> clusters = new ArrayList<>();
        for (JsonElement elem : clustersArray) {
            JsonObject clusterObj = elem.getAsJsonObject();
            String clusterId = clusterObj.get("clusterId").getAsString();
            String endpoint = clusterObj.get("endpoint").getAsString();
            int capability = clusterObj.get("capability").getAsInt();
            clusters.add(new ClusterInfo(clusterId, endpoint, capability));
        }

        return new GlobalTopology(version, clusters);
    }

    private static void sleepBackoff(int attempt) {
        long backoff = BASE_BACKOFF_MS * (1L << (attempt - 1)); // exponential: 1s, 2s, 4s...
        backoff = Math.min(backoff, MAX_BACKOFF_MS);
        // Add 10% jitter
        long jitter = (long) (backoff * 0.1 * ThreadLocalRandom.current().nextDouble());
        try {
            Thread.sleep(backoff + jitter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching global topology", e);
        }
    }

    @FunctionalInterface
    interface DnsContextFactory {
        DirContext create(Hashtable<String, String> environment) throws NamingException;
    }

    /** Resolves a hostname to the SRV seed list. Empty list means deterministically not provisioned. */
    @FunctionalInterface
    interface SrvResolver {
        List<SrvTarget> resolve(String hostname);
    }

    /** Fetches topology from one seed. Any thrown exception is classified by the caller. */
    @FunctionalInterface
    interface SeedProber {
        GlobalTopology probe(SrvTarget target) throws Exception;
    }

    /** The outcome of probing a candidate seed set. */
    static final class ProbeOutcome {
        final GlobalTopology best;
        final GlobalClusterApiException apiError;
        final GlobalClusterApiException authenticationError;
        final boolean apiErrorsAgree;
        final Exception lastError;

        ProbeOutcome(GlobalTopology best, GlobalClusterApiException apiError,
                     GlobalClusterApiException authenticationError, boolean apiErrorsAgree,
                     Exception lastError) {
            this.best = best;
            this.apiError = apiError;
            this.authenticationError = authenticationError;
            this.apiErrorsAgree = apiErrorsAgree;
            this.lastError = lastError;
        }
    }

    private static final class ProbeResult {
        final SrvTarget target;
        final GlobalTopology topology;
        final Exception error;

        ProbeResult(SrvTarget target, GlobalTopology topology, Exception error) {
            this.target = target;
            this.topology = topology;
            this.error = error;
        }
    }
}
