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

import com.google.gson.Gson;
import io.milvus.bulkwriter.request.stage.CreateStageRequest;
import io.milvus.bulkwriter.request.stage.DeleteStageRequest;
import io.milvus.bulkwriter.request.stage.ListStagesRequest;
import io.milvus.bulkwriter.response.stage.ListStagesResponse;
import io.milvus.bulkwriter.restful.DataStageUtils;

public class StageManager {
    private final String cloudEndpoint;
    private final String apiKey;

    public StageManager(StageManagerParam stageWriterParam) {
        cloudEndpoint = stageWriterParam.getCloudEndpoint();
        apiKey = stageWriterParam.getApiKey();
    }

    /**
     * Create a stage under the specified project and regionId.
     */
    public void createStage(CreateStageRequest request) {
        DataStageUtils.createStage(cloudEndpoint, apiKey, request);
    }

    /**
     * Delete a stage.
     */
    public void deleteStage(DeleteStageRequest request) {
        DataStageUtils.deleteStage(cloudEndpoint, apiKey, request);
    }

    /**
     * Paginated query of the stage list under a specified projectId.
     */
    public ListStagesResponse listStages(ListStagesRequest request) {
        String result = DataStageUtils.listStages(cloudEndpoint, apiKey, request);
        return new Gson().fromJson(result, ListStagesResponse.class);
    }
}