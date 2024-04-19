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

public enum CloudStorage {
    MINIO("%s", "minioAddress"),
    AWS("s3.amazonaws.com", null),
    GCP("storage.googleapis.com", null),
    AZURE("%s.blob.core.windows.net", "accountName"),
    ALI("oss-%s.aliyuncs.com", "region"),
    TC("cos.%s.myqcloud.com", "region")
    ;

    private String endpoint;

    private String replace;

    CloudStorage(String endpoint, String replace) {
        this.endpoint = endpoint;
        this.replace = replace;
    }

    public String getEndpoint(String... replaceParams) {
        if (StringUtils.isEmpty(replace))  {
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
                return String.format("https://%s.cos.%s.myqcloud.com/%s", bucketName, region, commonPrefix);
            case ALI:
                return String.format("https://%s.oss-%s.aliyuncs.com/%s", bucketName, region, commonPrefix);
            default:
                throw new ParamException("no support others remote storage address");
        }
    }

    public String getAzureObjectUrl(String accountName, String containerName, String commonPrefix) {
        if (this == CloudStorage.AZURE) {
            return String.format("https://%s.blob.core.windows.net/%s/%s", accountName, containerName, commonPrefix);
        }
        throw new ParamException("no support others remote storage address");
    }
}
