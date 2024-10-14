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
import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.request.BulkImportRequest;
import io.milvus.bulkwriter.request.GetImportProgressRequest;
import io.milvus.bulkwriter.request.ListImportJobsRequest;
import io.milvus.bulkwriter.response.RestfulResponse;

import java.util.Map;

public class CloudImport extends BaseCloudImport {
    private static final Gson GSON_INSTANCE = new Gson();

    public static String bulkImport(String url, String apiKey, BulkImportRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/create";

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<Object>>(){}.getType());
        handleResponse(requestURL, response);
        return body;
    }

    public static String getImportProgress(String url, String apiKey, GetImportProgressRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/describe";

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<Object>>(){}.getType());
        handleResponse(requestURL, response);
        return body;
    }

    public static String listImportJobs(String url, String apiKey, ListImportJobsRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/list";

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<Object>>(){}.getType());
        handleResponse(requestURL, response);
        return body;
    }

}
