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

package io.milvus.bulkwriter.connect;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.minio.credentials.Credentials;
import io.minio.credentials.Provider;
import io.minio.messages.ResponseDate;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

/**
 * A minio {@link Provider} that fetches OAuth2 access tokens from the GCE metadata
 * server, for workloads running on GCP with a service-account-attached identity (GKE
 * workload identity, GCE service account).
 *
 * <p>The GCS XML API authenticates with an {@code Authorization: Bearer} header instead
 * of request signing, so the access token is carried in {@link Credentials#sessionToken()}
 * (matching the established convention of passing the GCS bearer token as the session
 * token); accessKey/secretKey hold a sentinel value and are never used.</p>
 *
 * <p>Tokens are cached and refetched shortly before the server-reported expiry, so a
 * long-running writer never sends an expired token. If a refetch fails while the cached
 * token is still valid, the cached token is served until it truly expires; the cached
 * fast path is lock-free, since {@code fetch()} is invoked per storage request.</p>
 */
public class GcpMetadataServerCredentialsProvider implements Provider {
    private static final String TOKEN_URL =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    // refetch this far ahead of the server-reported expiry to absorb clock skew and
    // in-flight requests
    private static final long REFRESH_MARGIN_MS = 60_000;
    // minio Credentials rejects empty accessKey/secretKey; the bearer path never reads
    // them, so a self-describing sentinel fills the slots
    private static final String BEARER_ONLY = "gcp-bearer-only";

    private final String tokenUrl;
    private final OkHttpClient httpClient;

    private volatile Credentials cachedCredentials;
    private volatile long expiresAtMillis;

    public GcpMetadataServerCredentialsProvider() {
        this(TOKEN_URL, new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build());
    }

    // visible for testing
    GcpMetadataServerCredentialsProvider(String tokenUrl, OkHttpClient httpClient) {
        this.tokenUrl = tokenUrl;
        this.httpClient = httpClient;
    }

    @Override
    public Credentials fetch() {
        if (cachedCredentials != null && System.currentTimeMillis() < expiresAtMillis - REFRESH_MARGIN_MS) {
            return cachedCredentials;
        }
        synchronized (this) {
            long now = System.currentTimeMillis();
            if (cachedCredentials != null && now < expiresAtMillis - REFRESH_MARGIN_MS) {
                return cachedCredentials;
            }
            try {
                return fetchToken();
            } catch (IllegalStateException e) {
                if (cachedCredentials != null && now < expiresAtMillis) {
                    // the cached token is still valid; serve it while the metadata
                    // server is unreachable instead of failing in-flight uploads
                    return cachedCredentials;
                }
                throw e;
            }
        }
    }

    private Credentials fetchToken() {
        Request request = new Request.Builder()
                .url(tokenUrl)
                .header("Metadata-Flavor", "Google")
                .get()
                .build();
        String body;
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful() || response.body() == null) {
                throw tokenFetchFailure("http " + response.code(), null);
            }
            body = response.body().string();
        } catch (IOException e) {
            throw tokenFetchFailure(e.getMessage(), e);
        }
        try {
            JsonObject json = JsonParser.parseString(body).getAsJsonObject();
            String token = json.get("access_token").getAsString();
            long expiresInSeconds = json.get("expires_in").getAsLong();
            cachedCredentials = new Credentials(BEARER_ONLY, BEARER_ONLY, token,
                    new ResponseDate(ZonedDateTime.now().plusSeconds(expiresInSeconds)));
            expiresAtMillis = System.currentTimeMillis() + expiresInSeconds * 1000;
            return cachedCredentials;
        } catch (RuntimeException e) {
            // malformed JSON or missing access_token/expires_in fields
            throw tokenFetchFailure("unparseable token response: " + e.getMessage(), e);
        }
    }

    private static IllegalStateException tokenFetchFailure(String detail, Throwable cause) {
        String message = "failed to fetch GCP access token from metadata server, " + detail
                + "; no GCP service account identity is available in this environment";
        return cause == null ? new IllegalStateException(message) : new IllegalStateException(message, cause);
    }
}
