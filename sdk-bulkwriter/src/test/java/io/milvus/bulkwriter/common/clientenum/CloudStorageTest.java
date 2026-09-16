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

package io.milvus.bulkwriter.common.clientenum;

import io.milvus.exception.ParamException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class CloudStorageTest {

    @Test
    void testCloudNameResolution() {
        assertEquals("minio", CloudStorage.MINIO.getCloudName());
        assertEquals("aws", CloudStorage.AWS.getCloudName());
        assertEquals("gcp", CloudStorage.GCP.getCloudName());
        assertEquals("az", CloudStorage.AZ.getCloudName());
        assertEquals("ali", CloudStorage.ALI.getCloudName());
        assertEquals("tc", CloudStorage.TC.getCloudName());

        assertEquals(CloudStorage.MINIO, CloudStorage.getCloudStorage("minio"));
        assertEquals(CloudStorage.AWS, CloudStorage.getCloudStorage("aws"));
        assertEquals(CloudStorage.AZURE, CloudStorage.getCloudStorage("azure"));
        assertEquals(CloudStorage.ALIYUN, CloudStorage.getCloudStorage("aliyun"));
        assertEquals(CloudStorage.TENCENT, CloudStorage.getCloudStorage("tencent"));
    }

    @Test
    void testCloudFamilyPredicates() {
        assertTrue(CloudStorage.isAliCloud("ali"));
        assertTrue(CloudStorage.isAliCloud("aliyun"));
        assertTrue(CloudStorage.isAliCloud("alibaba"));
        assertTrue(CloudStorage.isAliCloud("alicloud"));
        assertFalse(CloudStorage.isAliCloud("aws"));

        assertTrue(CloudStorage.isTcCloud("tc"));
        assertTrue(CloudStorage.isTcCloud("tencent"));
        assertFalse(CloudStorage.isTcCloud("aws"));

        assertTrue(CloudStorage.isGcpCloud("gcp"));
        assertFalse(CloudStorage.isGcpCloud("aws"));

        assertTrue(CloudStorage.isAzCloud("az"));
        assertTrue(CloudStorage.isAzCloud("azure"));
        assertFalse(CloudStorage.isAzCloud("aws"));
    }

    @Test
    void testGetCloudStorageRejectsUnknownName() {
        assertThrows(ParamException.class, () -> CloudStorage.getCloudStorage("unknown-cloud"));
    }

    @Test
    void testGetEndpoint() {
        assertEquals("s3.amazonaws.com", CloudStorage.AWS.getEndpoint());
        assertEquals("storage.googleapis.com", CloudStorage.GCP.getEndpoint());
        assertEquals("minio-address", CloudStorage.MINIO.getEndpoint("minio-address"));
        assertEquals("my-account.blob.core.windows.net", CloudStorage.AZURE.getEndpoint("my-account"));
        assertEquals("oss-cn-hangzhou.aliyuncs.com", CloudStorage.ALIYUN.getEndpoint("cn-hangzhou"));
        assertEquals("cos.ap-guangzhou.myqcloud.com", CloudStorage.TENCENT.getEndpoint("ap-guangzhou"));
    }

    @Test
    void testGetEndpointRequiresReplaceParams() {
        assertThrows(ParamException.class, () -> CloudStorage.MINIO.getEndpoint());
        assertThrows(ParamException.class, () -> CloudStorage.AZURE.getEndpoint());
        assertThrows(ParamException.class, () -> CloudStorage.ALIYUN.getEndpoint());
    }

    @Test
    void testGetS3ObjectUrl() {
        assertEquals("https://s3.us-west-2.amazonaws.com/bucket/data/",
                CloudStorage.AWS.getS3ObjectUrl("bucket", "data/", "us-west-2"));
        assertEquals("https://storage.cloud.google.com/bucket/data/",
                CloudStorage.GCP.getS3ObjectUrl("bucket", "data/", "us-west-2"));
        assertEquals("https://bucket.cos.ap-guangzhou.myqcloud.com/data/",
                CloudStorage.TC.getS3ObjectUrl("bucket", "data/", "ap-guangzhou"));
        assertEquals("https://bucket.oss-cn-hangzhou.aliyuncs.com/data/",
                CloudStorage.ALICLOU.getS3ObjectUrl("bucket", "data/", "cn-hangzhou"));
    }

    @Test
    void testGetS3ObjectUrlRejectsUnsupportedCloud() {
        assertThrows(ParamException.class, () -> CloudStorage.MINIO.getS3ObjectUrl("bucket", "data/", "us-west-2"));
        assertThrows(ParamException.class, () -> CloudStorage.AZURE.getS3ObjectUrl("bucket", "data/", "us-west-2"));
    }

    @Test
    void testGetAzureObjectUrl() {
        assertEquals("https://my-account.blob.core.windows.net/container/data/",
                CloudStorage.AZ.getAzureObjectUrl("my-account", "container", "data/"));
        assertEquals("https://my-account.blob.core.windows.net/container/data/",
                CloudStorage.AZURE.getAzureObjectUrl("my-account", "container", "data/"));
    }

    @Test
    void testGetAzureObjectUrlRejectsNonAzureCloud() {
        assertThrows(ParamException.class, () -> CloudStorage.AWS.getAzureObjectUrl("my-account", "container", "data/"));
        assertThrows(ParamException.class, () -> CloudStorage.MINIO.getAzureObjectUrl("my-account", "container", "data/"));
    }

    @Test
    void testEnumValues() {
        Assertions.assertEquals(11, CloudStorage.values().length);
        Assertions.assertEquals(CloudStorage.ALIBABA, CloudStorage.valueOf("ALIBABA"));
    }
}
