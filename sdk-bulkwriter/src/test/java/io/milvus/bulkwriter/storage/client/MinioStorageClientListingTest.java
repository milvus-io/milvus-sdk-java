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

package io.milvus.bulkwriter.storage.client;

import io.minio.errors.ErrorResponseException;
import io.milvus.bulkwriter.storage.StorageClient.ObjectListPage;
import java.io.IOException;
import java.util.Arrays;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class MinioStorageClientListingTest {
    private static final String KEY = "volume-prefix/data/a.json";
    private final List<OkHttpClient> httpClients = new ArrayList<>();

    @AfterEach
    void closeHttpClients() {
        for (OkHttpClient client : httpClients) {
            client.dispatcher().executorService().shutdownNow();
            client.connectionPool().evictAll();
        }
    }

    @Test
    void closingOwnedClientCancelsInFlightHttpCalls() throws Exception {
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch finished = new java.util.concurrent.CountDownLatch(1);
        OkHttpClient http = new OkHttpClient.Builder().addInterceptor(chain -> {
            started.countDown();
            while (!chain.call().isCanceled()) {
                try { Thread.sleep(10); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
            }
            throw new IOException("cancelled");
        }).build();
        httpClients.add(http);
        MinioStorageClient client = new MinioStorageClient(io.minio.MinioAsyncClient.builder()
                .endpoint("https://example.com").httpClient(http).build(), true);
        okhttp3.Call call = http.newCall(new Request.Builder().url("https://example.com/bucket").build());
        call.enqueue(new okhttp3.Callback() {
            public void onFailure(okhttp3.Call call, IOException failure) { finished.countDown(); }
            public void onResponse(okhttp3.Call call, Response response) { response.close(); finished.countDown(); }
        });
        try {
            assertTrue(started.await(10, java.util.concurrent.TimeUnit.SECONDS));
            client.close();
            assertTrue(call.isCanceled());
            assertTrue(finished.await(10, java.util.concurrent.TimeUnit.SECONDS));
        } finally { call.cancel(); }
    }

    @Test
    void closingBorrowedClientDoesNotShutDownItsExecutor() {
        OkHttpClient http = new OkHttpClient();
        httpClients.add(http);
        MinioStorageClient client = new MinioStorageClient(io.minio.MinioAsyncClient.builder()
                .endpoint("https://example.com").httpClient(http).build(), false);
        client.close();
        assertFalse(http.dispatcher().executorService().isShutdown());
    }

    @Test
    void emptyListingMeansAbsent() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200, page(false));
        assertNull(client.findObject("bucket", KEY));
        assertScopedList(requests.get(0), KEY);
        assertEquals(1, requests.size());
    }

    @Test
    void exactZeroByteObjectExistsWithoutHeadOrGetObject() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200, page(false, KEY));
        assertNotNull(client.findObject("bucket", KEY));
        assertScopedList(requests.get(0), KEY);
        assertEquals(1, requests.size());
    }

    @Test
    void samePrefixFilesAndChildrenAreNotExactMatches() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200,
                page(false, KEY + ".bak", KEY + "/", KEY + "/child"));
        assertNull(client.findObject("bucket", KEY));
        assertScopedList(requests.get(0), KEY);
    }

    @Test
    void followsPaginationUntilExactMatchWithoutAssumingOrder() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200,
                page(true, KEY + ".bak"), page(false, KEY));
        assertNotNull(client.findObject("bucket", KEY));
        assertEquals(2, requests.size());
        assertNull(requests.get(0).url().queryParameter("continuation-token"));
        assertEquals("next-page", requests.get(1).url().queryParameter("continuation-token"));
        for (Request request : requests) {
            assertScopedList(request, KEY);
        }
    }

    @Test
    void exhaustsAllPagesBeforeReportingAbsence() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200,
                page(true, KEY + ".bak"), page(false, KEY + "/child"));
        assertNull(client.findObject("bucket", KEY));
        assertEquals(2, requests.size());
    }

    @Test
    void encodedKeysAreComparedAfterDecoding() throws Exception {
        String key = "volume-prefix/data/中文 +&%.json";
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200, page(false, key));
        assertNotNull(client.findObject("bucket", key));
        assertScopedList(requests.get(0), key);
    }

    @Test
    void listingErrorsArePropagatedInsteadOfReportingAbsence() {
        for (String code : new String[]{"AccessDenied", "NoSuchBucket"}) {
            List<Request> requests = new ArrayList<>();
            MinioStorageClient client = client(requests, code.equals("AccessDenied") ? 403 : 404,
                    "<Error><Code>" + code + "</Code><Message>test error</Message></Error>");
            ErrorResponseException error = assertThrows(ErrorResponseException.class,
                    () -> client.findObject("bucket", KEY));
            assertEquals(code, error.errorResponse().code());
            assertScopedList(requests.get(0), KEY);
        }
    }

    @Test
    void batchListingFetchesOnlyOnePageWithMaximumPageSize() throws Exception {
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200, page(true, KEY, KEY + ".bak"));
        ObjectListPage result = client.listObjectsPage("bucket", "volume-prefix/data/", null);
        assertEquals(Arrays.asList(KEY, KEY + ".bak"), result.getObjects().stream().map(io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata::getKey).collect(java.util.stream.Collectors.toList()));
        assertEquals(Long.valueOf(0), result.getObjects().get(0).getSize());
        assertEquals(Long.valueOf(java.time.Instant.parse("2026-09-17T00:00:00Z").toEpochMilli()),
                result.getObjects().get(0).getLastModifiedTimeMillis());
        assertEquals("next-page", result.getNextContinuationToken());
        assertEquals(1, requests.size());
        assertScopedList(requests.get(0), "volume-prefix/data/");
        assertEquals("1000", requests.get(0).url().queryParameter("max-keys"));
    }

    @Test
    void batchListingUsesCursorAndDecodesKeys() throws Exception {
        String key = "volume-prefix/data/中文 +&%.json";
        List<Request> requests = new ArrayList<>();
        MinioStorageClient client = client(requests, 200, page(false, key));
        ObjectListPage result = client.listObjectsPage("bucket", "volume-prefix/data/", "next-page");
        assertEquals(Arrays.asList(key), result.getObjects().stream().map(io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata::getKey).collect(java.util.stream.Collectors.toList()));
        assertNull(result.getNextContinuationToken());
        assertEquals("next-page", requests.get(0).url().queryParameter("continuation-token"));
        assertScopedList(requests.get(0), "volume-prefix/data/");
    }

    @Test
    void emptyBatchListingIsTheFinalPage() throws Exception {
        List<Request> requests = new ArrayList<>();
        ObjectListPage result = client(requests, 200, page(false))
                .listObjectsPage("bucket", "volume-prefix/data/", null);
        assertTrue(result.getObjects().stream().map(io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata::getKey).collect(java.util.stream.Collectors.toList()).isEmpty());
        assertNull(result.getNextContinuationToken());
        assertEquals(1, requests.size());
    }

    @Test
    void invalidContinuationTokensCannotSilentlyTruncateOrLoop() throws Exception {
        List<Request> requests = new ArrayList<>();
        String missingToken = page(true, KEY).replace(
                "<NextContinuationToken>next-page</NextContinuationToken>", "");
        MinioStorageClient missing = client(requests, 200, missingToken);
        assertThrows(IOException.class, () -> missing.listObjectsPage("bucket", "volume-prefix/data/", null));
        MinioStorageClient repeated = client(requests, 200, page(true, KEY));
        assertThrows(IOException.class, () -> repeated.listObjectsPage("bucket", "volume-prefix/data/", "next-page"));
    }

    @Test
    void missingMetadataRemainsUnknownInsteadOfZero() throws Exception {
        List<Request> requests = new ArrayList<>();
        String xml = page(false, KEY).replace("<Size>0</Size>", "")
                .replace("<LastModified>2026-09-17T00:00:00.000Z</LastModified>", "");
        io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata object =
                client(requests, 200, xml).findObject("bucket", KEY);
        assertNotNull(object);
        assertNull(object.getSize());
        assertNull(object.getLastModifiedTimeMillis());
    }

    @Test
    void preservesLargeSizesAndSubsecondTimestamps() throws Exception {
        List<Request> requests = new ArrayList<>();
        String xml = page(false, KEY).replace("<Size>0</Size>", "<Size>5368709120</Size>")
                .replace("00:00:00.000Z", "00:00:00.123Z");
        io.milvus.bulkwriter.storage.StorageClient.ObjectMetadata object =
                client(requests, 200, xml).findObject("bucket", KEY);
        assertEquals(Long.valueOf(5368709120L), object.getSize());
        assertEquals(Long.valueOf(java.time.Instant.parse("2026-09-17T00:00:00.123Z").toEpochMilli()),
                object.getLastModifiedTimeMillis());
    }

    private static void assertScopedList(Request request, String key) {
        assertEquals("GET", request.method());
        assertEquals("/bucket", request.url().encodedPath());
        assertEquals("2", request.url().queryParameter("list-type"));
        assertEquals(key, request.url().queryParameter("prefix"));
        String delimiter = request.url().queryParameter("delimiter");
        assertTrue(delimiter == null || delimiter.isEmpty());
    }

    private MinioStorageClient client(List<Request> requests, int status, String... pages) {
        AtomicInteger calls = new AtomicInteger();
        // Intercept real MinIO-generated HTTP requests without sending anything to the network.
        OkHttpClient httpClient = new OkHttpClient.Builder().addInterceptor(chain -> {
            Request request = chain.request();
            requests.add(request);
            int index = calls.getAndIncrement();
            if (index >= pages.length) {
                throw new AssertionError("Unexpected extra request: " + request);
            }
            return new Response.Builder().request(request).protocol(Protocol.HTTP_1_1)
                    .code(status).message("test response").header("Content-Type", "application/xml")
                    .body(ResponseBody.create(MediaType.parse("application/xml"), pages[index])).build();
        }).build();
        httpClients.add(httpClient);
        return MinioStorageClient.getStorageClient("aws", "https://storage.example.com",
                "access-key", "secret-key", "session-token", "us-east-1", httpClient);
    }

    private static String page(boolean truncated, String... keys) throws Exception {
        StringBuilder xml = new StringBuilder("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
                .append("<Name>bucket</Name><EncodingType>url</EncodingType><IsTruncated>")
                .append(truncated).append("</IsTruncated>");
        if (truncated) {
            xml.append("<NextContinuationToken>next-page</NextContinuationToken>");
        }
        for (String key : keys) {
            xml.append("<Contents><Key>").append(URLEncoder.encode(key, "UTF-8"))
                    .append("</Key><LastModified>2026-09-17T00:00:00.000Z</LastModified><Size>0</Size></Contents>");
        }
        return xml.append("</ListBucketResult>").toString();
    }
}
