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

import io.milvus.bulkwriter.StageOperation;
import io.milvus.bulkwriter.StageOperationParam;
import io.milvus.bulkwriter.model.StageUploadResult;


/**
 * if you don't have bucket, but you want to upload data to bucket and import to milvus
 * you can use this function
 */
public class StageExample {
    /**
     * You need to upload the local file path or folder path for import.
     */
    public static final String LOCAL_DIR_OR_FILE_PATH = "/Users/zilliz/Desktop/1.parquet";

    /**
     * The value of the URL is fixed.
     * For overseas regions, it is: https://api.cloud.zilliz.com
     * For regions in China, it is: https://api.cloud.zilliz.com.cn
     */
    public static final String CLOUD_ENDPOINT = "https://api.cloud.zilliz.com";
    public static final String API_KEY = "_api_key_for_cluster_org_";
    /**
     * This is currently a private preview feature. If you need to use it, please submit a request and contact us.
     * Before using this feature, you need to create a stage using the stage API.
     */
    public static final String STAGE_NAME = "_stage_name_for_project_";
    public static final String PATH = "_path_for_stage";

    public static void main(String[] args) throws Exception {
        uploadFileToStage();
    }

    /**
     * If you want to upload file to stage, and then use cloud interface merge the data to collection
     */
    private static void uploadFileToStage() throws Exception {
        StageOperationParam stageOperationParam = StageOperationParam.newBuilder()
                .withCloudEndpoint(CLOUD_ENDPOINT).withApiKey(API_KEY)
                .withStageName(STAGE_NAME).withPath(PATH)
                .build();
        StageOperation stageOperation = new StageOperation(stageOperationParam);
        StageUploadResult result = stageOperation.uploadFileToStage(LOCAL_DIR_OR_FILE_PATH);
        System.out.println("\nuploadFileToStage results: " + result);
    }
}
