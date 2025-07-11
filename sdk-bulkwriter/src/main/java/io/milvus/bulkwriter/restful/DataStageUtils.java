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

package io.milvus.bulkwriter.restful;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.request.stage.BaseStageRequest;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;

import java.util.Map;

public class DataStageUtils extends BaseRestful {
    public static String applyStage(String url, BaseStageRequest request) {
        String requestURL = url + "/v2/stages/apply";

        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, request.getApiKey(), params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>(){}.getType());
        handleResponse(requestURL, response);
        return new Gson().toJson(response.getData());
    }
}
