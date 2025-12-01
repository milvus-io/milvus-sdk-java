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
package io.milvus.v2;

import com.google.gson.Gson;
import io.milvus.bulkwriter.VolumeFileManager;
import io.milvus.bulkwriter.VolumeFileManagerParam;
import io.milvus.bulkwriter.common.clientenum.ConnectType;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.request.volume.UploadFilesRequest;


public class VolumeFileManagerExample {
    private static final VolumeFileManager volumeFileManager;

    static {
        VolumeFileManagerParam volumeFileManagerParam = VolumeFileManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("_api_key_for_cluster_org_")
                .withVolumeName("_volume_name_for_project_")
                .withConnectType(ConnectType.AUTO)
                .build();
        volumeFileManager = new VolumeFileManager(volumeFileManagerParam);
    }

    public static void main(String[] args) throws Exception {
        uploadFiles();
        shutdown();
    }

    private static void uploadFiles() throws Exception {
        UploadFilesRequest request = UploadFilesRequest.builder()
                .sourceFilePath("/Users/zilliz/data/")
                .targetVolumePath("data/")
                .build();
        UploadFilesResult result = volumeFileManager.uploadFilesAsync(request).get();
        System.out.println("\nuploadFiles results: " + new Gson().toJson(result));
    }

    private static void shutdown() {
        volumeFileManager.shutdownGracefully();
    }
}
