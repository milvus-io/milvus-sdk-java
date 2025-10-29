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

public enum CloudStorage {
    MINIO("minio", "%s", "minioAddress"),
    AWS("aws", "s3.amazonaws.com", null),
    GCP("gcp", "storage.googleapis.com", null),

    AZ("az", "%s.blob.core.windows.net", "accountName"),
    AZURE("azure", "%s.blob.core.windows.net", "accountName"),

    ALI("ali", "oss-%s.aliyuncs.com", "region"),
    ALIYUN("aliyun", "oss-%s.aliyuncs.com", "region"),
    ALIBABA("alibaba", "oss-%s.aliyuncs.com", "region"),
    ALICLOU("alicloud", "oss-%s.aliyuncs.com", "region"),

    TC("tc", "cos.%s.myqcloud.com", "region"),
    TENCENT("tencent", "cos.%s.myqcloud.com", "region");

    private final String cloudName;

    private final String endpoint;

    private final String replace;

    CloudStorage(String cloudName, String endpoint, String replace) {
        this.cloudName = cloudName;
        this.endpoint = endpoint;
        this.replace = replace;
    }

    public String getCloudName() {
        return cloudName;
    }

    public static boolean isAliCloud(String cloudName) {
        List<CloudStorage> aliCloudStorages = Lists.newArrayList(
                CloudStorage.ALI, CloudStorage.ALIYUN, CloudStorage.ALIBABA, CloudStorage.ALICLOU
        );
        return aliCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    public static boolean isTcCloud(String cloudName) {
        List<CloudStorage> tcCloudStorages = Lists.newArrayList(
                CloudStorage.TC, CloudStorage.TENCENT
        );
        return tcCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    public static boolean isAzCloud(String cloudName) {
        List<CloudStorage> azCloudStorages = Lists.newArrayList(
                CloudStorage.AZ, CloudStorage.AZURE
        );
        return azCloudStorages.stream().anyMatch(e -> e.getCloudName().equalsIgnoreCase(cloudName));
    }

    public static CloudStorage getCloudStorage(String cloudName) {
        for (CloudStorage cloudStorage : values()) {
            if (cloudStorage.getCloudName().equals(cloudName)) {
                return cloudStorage;
            }
        }
        throw new ParamException("no support others cloudName");
    }

    public String getEndpoint(String... replaceParams) {
        if (StringUtils.isEmpty(replace)) {
            return endpoint;
        }
        if (replaceParams.length == 0) {
            throw new ParamException(String.format("Please input the replaceParams:%s when you want to get endpoint of %s", replace, this.name()));
        }
        return String.format(endpoint, replaceParams[0]);
    }

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

    public String getAzureObjectUrl(String accountName, String containerName, String commonPrefix) {
        if (CloudStorage.isAzCloud(this.getCloudName())) {
            return String.format("https://%s.blob.core.windows.net/%s/%s", accountName, containerName, commonPrefix);
        }
        throw new ParamException("no support others remote storage address");
    }
}
