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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GlobalClusterUtils {
    private static final Logger logger = LoggerFactory.getLogger(GlobalClusterUtils.class);

    private static final String GLOBAL_CLUSTER_MARKER = "global-cluster";
    private static final String TOPOLOGY_PATH = "/global-cluster/topology";
    private static final int MAX_RETRIES = 3;
    private static final long BASE_BACKOFF_MS = 1000;
    private static final long MAX_BACKOFF_MS = 10000;
    private static final int REQUEST_TIMEOUT_MS = 10000;

    private GlobalClusterUtils() {
    }

    public static boolean isGlobalEndpoint(String uri) {
        if (uri == null) {
            return false;
        }
        return uri.toLowerCase().contains(GLOBAL_CLUSTER_MARKER);
    }

    public static GlobalTopology fetchTopology(String globalEndpoint, String token) {
        String topologyUrl = buildTopologyUrl(globalEndpoint);

        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                String responseBody = doHttpGet(topologyUrl, token);
                return parseTopologyResponse(responseBody);
            } catch (Exception e) {
                lastException = e;
                logger.warn("Failed to fetch global topology (attempt {}/{}): {}", attempt, MAX_RETRIES, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    long backoff = calculateBackoff(attempt);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while fetching global topology", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Failed to fetch global topology after " + MAX_RETRIES + " attempts", lastException);
    }

    static String buildTopologyUrl(String globalEndpoint) {
        // Normalize the URI: ensure HTTPS scheme, trim whitespace, remove trailing slash.
        // The host and port are preserved as-is — the topology REST API is expected to be
        // served on the same host:port as the global endpoint.
        // Example: "https://xxx.global-cluster.yyy.com:443"
        //       -> "https://xxx.global-cluster.yyy.com:443/global-cluster/topology"
        String base = globalEndpoint.trim();
        if (!base.startsWith("http://") && !base.startsWith("https://")) {
            base = "https://" + base;
        }
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        // Upgrade http to https — the topology API requires TLS
        if (base.startsWith("http://")) {
            base = "https://" + base.substring(7);
        }
        return base + TOPOLOGY_PATH;
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
                throw new IOException("HTTP request failed with status code: " + responseCode);
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }
            return response.toString();
        } finally {
            conn.disconnect();
        }
    }

    static GlobalTopology parseTopologyResponse(String responseBody) {
        JsonObject root = JsonParser.parseString(responseBody).getAsJsonObject();
        int code = root.get("code").getAsInt();
        if (code != 0) {
            String message = root.has("message") ? root.get("message").getAsString() : "unknown error";
            throw new RuntimeException("Global topology API returned error code " + code + ": " + message);
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

    private static long calculateBackoff(int attempt) {
        long backoff = BASE_BACKOFF_MS * (1L << (attempt - 1)); // exponential: 1s, 2s, 4s...
        backoff = Math.min(backoff, MAX_BACKOFF_MS);
        // Add 10% jitter
        long jitter = (long) (backoff * 0.1 * ThreadLocalRandom.current().nextDouble());
        return backoff + jitter;
    }
}
