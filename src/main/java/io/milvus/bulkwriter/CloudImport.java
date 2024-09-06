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
import io.milvus.bulkwriter.response.BulkImportResponse;
import io.milvus.bulkwriter.response.GetImportProgressResponse;
import io.milvus.bulkwriter.response.ListImportJobsResponse;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.bulkwriter.response.v2.GetImportProgressV2Response;
import io.milvus.bulkwriter.response.v2.ListImportJobsV2Response;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

@Deprecated
// use CloudImportV2 replace
public class CloudImport extends BaseCloudImport {
    private static final Gson GSON_INSTANCE = new Gson();

    public static BulkImportResponse bulkImport(String url, String apiKey, String objectUrl,
                                                String accessKey, String secretKey, String clusterId, String collectionName) throws MalformedURLException {
        url = convertToV2ControlBaseURL(url);
        String requestURL = url + "/v2/vectordb/jobs/import/create";

        Map<String, Object> params = new HashMap<>();
        params.put("objectUrl", objectUrl);
        params.put("accessKey", accessKey);
        params.put("secretKey", secretKey);
        params.put("clusterId", clusterId);
        params.put("collectionName", collectionName);

        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<BulkImportResponse> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<BulkImportResponse>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData();
    }

    public static GetImportProgressResponse getImportProgress(String url, String apiKey, String jobId, String clusterId) throws MalformedURLException {
        url = convertToV2ControlBaseURL(url);
        String requestURL = url + "/v2/vectordb/jobs/import/getProgress";

        Map<String, Object> params = new HashMap<>();
        params.put("clusterId", clusterId);
        params.put("jobId", jobId);

        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<GetImportProgressV2Response> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<GetImportProgressV2Response>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData().toGetImportProgressResponse();
    }

    public static ListImportJobsResponse listImportJobs(String url, String apiKey, String clusterId, int pageSize, int currentPage) throws MalformedURLException {
        url = convertToV2ControlBaseURL(url);
        String requestURL = url + "/v2/vectordb/jobs/import/list";

        Map<String, Object> params = new HashMap<>();
        params.put("clusterId", clusterId);
        params.put("pageSize", pageSize);
        params.put("currentPage", currentPage);

        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<ListImportJobsV2Response> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<ListImportJobsV2Response>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData().toListImportJobsResponse();
    }

}
