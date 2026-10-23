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

import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.request.describe.BaseDescribeImportRequest;
import io.milvus.bulkwriter.request.describe.MilvusDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.BaseImportRequest;
import io.milvus.bulkwriter.request.import_.CloudImportRequest;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.bulkwriter.request.list.BaseListImportJobsRequest;
import io.milvus.bulkwriter.request.list.MilvusListImportJobsRequest;
import io.milvus.bulkwriter.request.volume.ApplyVolumeRequest;
import io.milvus.bulkwriter.request.volume.BaseVolumeRequest;
import io.milvus.bulkwriter.response.ApplyVolumeResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CredentialRedactionTest {
    private static final String REDACTED = "<redacted>";

    @Test
    void s3ConnectParamRedactsCredentialsInToString() throws Exception {
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withBucketName("bucket")
                .withEndpoint("https://s3.example.com")
                .withAccessKey("ak-secret")
                .withSecretKey("sk-secret")
                .withSessionToken("st-secret")
                .build();

        String s = param.toString();
        assertFalse(s.contains("ak-secret"));
        assertFalse(s.contains("sk-secret"));
        assertFalse(s.contains("st-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void azureConnectParamRedactsConnStrInToString() throws Exception {
        AzureConnectParam param = AzureConnectParam.newBuilder()
                .withContainerName("container")
                .withConnStr("conn-secret")
                .withAccountUrl("https://account.blob.core.windows.net")
                .build();

        String s = param.toString();
        assertFalse(s.contains("conn-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void cloudImportRequestRedactsCredentialsInToString() {
        CloudImportRequest request = CloudImportRequest.builder()
                .apiKey("api-secret")
                .clusterId("cluster-0")
                .dbName("db1")
                .collectionName("coll1")
                .objectUrls(Collections.singletonList(Collections.singletonList("s3://bucket/file.parquet")))
                .accessKey("ak-secret")
                .secretKey("sk-secret")
                .token("obj-token-secret")
                .build();

        String s = request.toString();
        assertFalse(s.contains("api-secret"));
        assertFalse(s.contains("ak-secret"));
        assertFalse(s.contains("sk-secret"));
        assertFalse(s.contains("obj-token-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void baseImportRequestRedactsApiKeyInToString() {
        BaseImportRequest request = BaseImportRequest.builder()
                .apiKey("base-import-api-secret")
                .build();

        String s = request.toString();
        assertFalse(s.contains("base-import-api-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void baseVolumeRequestRedactsApiKeyInToString() {
        BaseVolumeRequest request = BaseVolumeRequest.builder()
                .apiKey("base-volume-api-secret")
                .build();

        String s = request.toString();
        assertFalse(s.contains("base-volume-api-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void baseDescribeImportRequestRedactsApiKeyInToString() {
        BaseDescribeImportRequest request = BaseDescribeImportRequest.builder()
                .apiKey("base-describe-api-secret")
                .build();

        String s = request.toString();
        assertFalse(s.contains("base-describe-api-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void baseListImportJobsRequestRedactsApiKeyInToString() {
        BaseListImportJobsRequest request = BaseListImportJobsRequest.builder()
                .apiKey("base-list-api-secret")
                .build();

        String s = request.toString();
        assertFalse(s.contains("base-list-api-secret"));
        assertTrue(s.contains(REDACTED));
    }

    @Test
    void subclassesDoNotLeakApiKeyInToString() {
        MilvusImportRequest importRequest = MilvusImportRequest.builder()
                .apiKey("sub-import-secret")
                .collectionName("coll1")
                .files(Collections.singletonList(Collections.singletonList("1.parquet")))
                .build();
        assertFalse(importRequest.toString().contains("sub-import-secret"));

        ApplyVolumeRequest volumeRequest = ApplyVolumeRequest.builder()
                .apiKey("sub-volume-secret")
                .volumeName("vol-1")
                .build();
        assertFalse(volumeRequest.toString().contains("sub-volume-secret"));

        MilvusDescribeImportRequest describeRequest = MilvusDescribeImportRequest.builder()
                .apiKey("sub-describe-secret")
                .jobId("job-1")
                .build();
        assertFalse(describeRequest.toString().contains("sub-describe-secret"));

        MilvusListImportJobsRequest listRequest = MilvusListImportJobsRequest.builder()
                .apiKey("sub-list-secret")
                .collectionName("coll1")
                .build();
        assertFalse(listRequest.toString().contains("sub-list-secret"));
    }

    @Test
    void applyVolumeResponseCredentialsRedactsTmpAkInToString() {
        ApplyVolumeResponse.Credentials credentials = ApplyVolumeResponse.Credentials.builder()
                .tmpAK("tmp-ak-secret")
                .tmpSK("tmp-sk-secret")
                .sessionToken("tmp-session-secret")
                .expireTime("2026-01-01T00:00:00Z")
                .build();

        String s = credentials.toString();
        assertFalse(s.contains("tmp-ak-secret"));
        assertTrue(s.contains(REDACTED));
    }
}
