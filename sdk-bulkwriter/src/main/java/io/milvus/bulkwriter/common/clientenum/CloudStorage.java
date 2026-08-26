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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Lists;

import java.util.List;

/**
 * Cloud storage providers supported by the Milvus BulkWriter, together with their endpoint templates
 * and the endpoint placeholder they replace.
 */
public enum CloudStorage {
    /** MinIO storage, endpoint built from the provided MinIO address. */
    MINIO("minio", "%s", "minioAddress"),
    /** Amazon Web Services S3 storage. */
    AWS("aws", "s3.amazonaws.com", null),
    /** Google Cloud Platform storage. */
    GCP("gcp", "storage.googleapis.com", null),

    /** Microsoft Azure Blob storage (short name). */
    AZ("az", "%s.blob.core.windows.net", "accountName"),
    /** Microsoft Azure Blob storage. */
    AZURE("azure", "%s.blob.core.windows.net", "accountName"),

    /** Alibaba Cloud OSS storage (short name). */
    ALI("ali", "oss-%s.aliyuncs.com", "region"),
    /** Alibaba Cloud OSS storage (alias). */
    ALIYUN("aliyun", "oss-%s.aliyuncs.com", "region"),
    /** Alibaba Cloud OSS storage (alias). */
    ALIBABA("alibaba", "oss-%s.aliyuncs.com", "region"),
    /** Alibaba Cloud OSS storage (alias). */
    ALICLOU("alicloud", "oss-%s.aliyuncs.com", "region"),

    /** Tencent Cloud COS storage (short name). */
    TC("tc", "cos.%s.myqcloud.com", "region"),
    /** Tencent Cloud COS storage. */
    TENCENT("tencent", "cos.%s.myqcloud.com", "region");

    private final String cloudName;

    private final String endpoint;

    private final String replace;

    CloudStorage(String cloudName, String endpoint, String replace) {
        this.cloudName = cloudName;
        this.endpoint = endpoint;
        this.replace = replace;
    }

    /**
     * Returns the cloud name of this storage provider.
     *
     * @return the cloud name
     */
    public String getCloudName() {
        return cloudName;
    }

    /**
     * Checks whether the given cloud name refers to an Alibaba Cloud OSS storage.
     *
     * @param cloudName the cloud name to check
     * @return {@code true} if the cloud name matches an Alibaba Cloud OSS storage
     */
    public static boolean isAliCloud(String cloudName) {
        List<CloudStorage> aliCloudStorages = Lists.newArrayList(
                CloudStorage.ALI, CloudStorage.ALIYUN, CloudStorage.ALIBABA, CloudStorage.ALICLOU
        );
        return aliCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    /**
     * Checks whether the given cloud name refers to a Tencent Cloud COS storage.
     *
     * @param cloudName the cloud name to check
     * @return {@code true} if the cloud name matches a Tencent Cloud COS storage
     */
    public static boolean isTcCloud(String cloudName) {
        List<CloudStorage> tcCloudStorages = Lists.newArrayList(
                CloudStorage.TC, CloudStorage.TENCENT
        );
        return tcCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    /**
     * Checks whether the given cloud name refers to a Google Cloud Platform storage.
     *
     * @param cloudName the cloud name to check
     * @return {@code true} if the cloud name matches a Google Cloud Platform storage
     */
    public static boolean isGcpCloud(String cloudName) {
        List<CloudStorage> gcpCloudStorages = Lists.newArrayList(
                CloudStorage.GCP
        );
        return gcpCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    /**
     * Checks whether the given cloud name refers to a Microsoft Azure Blob storage.
     *
     * @param cloudName the cloud name to check
     * @return {@code true} if the cloud name matches a Microsoft Azure Blob storage
     */
    public static boolean isAzCloud(String cloudName) {
        List<CloudStorage> azCloudStorages = Lists.newArrayList(
                CloudStorage.AZ, CloudStorage.AZURE
        );
        return azCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    /**
     * Resolves the {@link CloudStorage} enum constant for the given cloud name.
     *
     * @param cloudName the cloud name to resolve
     * @return the matching cloud storage constant
     * @throws io.milvus.exception.ParamException if no cloud storage matches the given cloud name
     */
    public static CloudStorage getCloudStorage(String cloudName) {
        for (CloudStorage cloudStorage : values()) {
            if (cloudStorage.getCloudName().equals(cloudName)) {
                return cloudStorage;
            }
        }
        throw new ParamException("no support others cloudName");
    }

    /**
     * Returns the endpoint of this cloud storage, filling the endpoint template with the given
     * replacement parameters when the provider requires them.
     *
     * @param replaceParams the parameters used to fill the endpoint template
     * @return the resolved endpoint
     * @throws io.milvus.exception.ParamException if the provider requires replacement parameters but none are given
     */
    public String getEndpoint(String... replaceParams) {
        if (StringUtils.isEmpty(replace)) {
            return endpoint;
        }
        if (replaceParams.length == 0) {
            throw new ParamException(String.format("Please input the replaceParams:%s when you want to get endpoint of %s", replace, this.name()));
        }
        return String.format(endpoint, replaceParams[0]);
    }

    /**
     * Returns the S3-compatible object URL for the given bucket, prefix and region on this cloud
     * storage provider.
     *
     * @param bucketName   the bucket name
     * @param commonPrefix the object key prefix
     * @param region       the cloud region
     * @return the S3-compatible object URL
     * @throws io.milvus.exception.ParamException if this provider does not support S3 object URLs
     */
    public String getS3ObjectUrl(String bucketName, String commonPrefix, String region) {
        switch (this) {
            case AWS:
                return String.format("https://s3.%s.amazonaws.com/%s/%s", region, bucketName, commonPrefix);
            case GCP:
                return String.format("https://storage.cloud.google.com/%s/%s", bucketName, commonPrefix);
            case TC:
            case TENCENT:
                return String.format("https://%s.cos.%s.myqcloud.com/%s", bucketName, region, commonPrefix);
            case ALI:
            case ALICLOU:
            case ALIBABA:
            case ALIYUN:
                return String.format("https://%s.oss-%s.aliyuncs.com/%s", bucketName, region, commonPrefix);
            default:
                throw new ParamException("no support others remote storage address");
        }
    }

    /**
     * Returns the Azure Blob object URL for the given account, container and prefix on this cloud
     * storage provider.
     *
     * @param accountName   the Azure storage account name
     * @param containerName the Azure container name
     * @param commonPrefix  the object key prefix
     * @return the Azure Blob object URL
     * @throws io.milvus.exception.ParamException if this provider is not an Azure cloud storage
     */
    public String getAzureObjectUrl(String accountName, String containerName, String commonPrefix) {
        if (CloudStorage.isAzCloud(this.getCloudName())) {
            return String.format("https://%s.blob.core.windows.net/%s/%s", accountName, containerName, commonPrefix);
        }
        throw new ParamException("no support others remote storage address");
    }
}
