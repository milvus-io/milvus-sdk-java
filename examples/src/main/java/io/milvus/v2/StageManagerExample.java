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
import io.milvus.bulkwriter.StageManager;
import io.milvus.bulkwriter.StageManagerParam;
import io.milvus.bulkwriter.request.stage.CreateStageRequest;
import io.milvus.bulkwriter.request.stage.DeleteStageRequest;
import io.milvus.bulkwriter.request.stage.ListStagesRequest;
import io.milvus.bulkwriter.response.stage.ListStagesResponse;


/**
 * This is currently a private preview feature. If you need to use it, please submit a request and contact us.
 */
public class StageManagerExample {
    private static final StageManager stageManager;
    static {
        StageManagerParam stageManagerParam = StageManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("_api_key_for_cluster_org_")
                .build();
        stageManager = new StageManager(stageManagerParam);
    }

    private static final String PROJECT_ID = "_id_for_project_";
    private static final String REGION_ID = "_id_for_region_";
    private static final String STAGE_NAME = "_stage_name_for_project_";

    public static void main(String[] args) throws Exception {
        createStage();
        listStages();
        deleteStage();
    }

    private static void createStage() {
        CreateStageRequest request = CreateStageRequest.builder()
                .projectId(PROJECT_ID).regionId(REGION_ID).stageName(STAGE_NAME)
                .build();
        stageManager.createStage(request);
        System.out.printf("\nStage %s created%n", STAGE_NAME);
    }

    private static void listStages() {
        ListStagesRequest request = ListStagesRequest.builder()
                .projectId(PROJECT_ID).currentPage(1).pageSize(10)
                .build();
        ListStagesResponse listStagesResponse = stageManager.listStages(request);
        System.out.println("\nlistStages results: " + new Gson().toJson(listStagesResponse));
    }

    private static void deleteStage() {
        DeleteStageRequest request = DeleteStageRequest.builder()
                .stageName(STAGE_NAME)
                .build();
        stageManager.deleteStage(request);
        System.out.printf("\nStage %s deleted%n", STAGE_NAME);
    }
}
