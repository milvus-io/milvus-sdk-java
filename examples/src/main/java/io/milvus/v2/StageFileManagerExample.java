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
import io.milvus.bulkwriter.StageFileManager;
import io.milvus.bulkwriter.StageFileManagerParam;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.request.stage.UploadFilesRequest;


/**
 * This is currently a private preview feature. If you need to use it, please submit a request and contact us.
 */
public class StageFileManagerExample {
    private static final StageFileManager stageFileManager;
    static {
        StageFileManagerParam stageFileManagerParam = StageFileManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("_api_key_for_cluster_org_")
                .withStageName("_stage_name_for_project_")
                .build();
        stageFileManager = new StageFileManager(stageFileManagerParam);
    }

    public static void main(String[] args) throws Exception {
        uploadFiles();
        shutdown();
    }

    private static void uploadFiles() throws Exception {
        UploadFilesRequest request = UploadFilesRequest.builder()
                .sourceFilePath("/Users/zilliz/data/")
                .targetStagePath("data/")
                .build();
        UploadFilesResult result = stageFileManager.uploadFilesAsync(request).get();
        System.out.println("\nuploadFiles results: " + new Gson().toJson(result));
    }

    private static void shutdown() {
        stageFileManager.shutdownGracefully();
    }
}
