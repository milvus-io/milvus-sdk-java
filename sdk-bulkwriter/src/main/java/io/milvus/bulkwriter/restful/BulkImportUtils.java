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

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.request.describe.BaseDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.BaseImportRequest;
import io.milvus.bulkwriter.request.list.BaseListImportJobsRequest;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;

import java.util.Map;

public class BulkImportUtils extends BaseRestful {
    private static final int CONNECT_TIMEOUT_MS = 60 * 1000;

    private static String postImportJobRequest(String requestURL, String apiKey, Object request) {
        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
        }.getType());
        String body = postRequest(requestURL, apiKey, params, CONNECT_TIMEOUT_MS);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
        return body;
    }

    public static String bulkImport(String url, BaseImportRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/create";
        return postImportJobRequest(requestURL, request.getApiKey(), request);
    }

    public static String getImportProgress(String url, BaseDescribeImportRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/describe";
        return postImportJobRequest(requestURL, request.getApiKey(), request);
    }

    public static String listImportJobs(String url, BaseListImportJobsRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/list";
        return postImportJobRequest(requestURL, request.getApiKey(), request);
    }

    public static String commitImport(String url, BaseDescribeImportRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/commit";
        return postImportJobRequest(requestURL, request.getApiKey(), request);
    }

    public static String abortImport(String url, BaseDescribeImportRequest request) {
        String requestURL = url + "/v2/vectordb/jobs/import/abort";
        return postImportJobRequest(requestURL, request.getApiKey(), request);
    }
}
