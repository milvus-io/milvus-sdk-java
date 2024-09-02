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
import io.milvus.bulkwriter.request.v2.BulkImportV2Request;
import io.milvus.bulkwriter.request.v2.GetImportProgressV2Request;
import io.milvus.bulkwriter.request.v2.ListImportJobsV2Request;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.bulkwriter.response.v2.BulkImportV2Response;
import io.milvus.bulkwriter.response.v2.GetImportProgressV2Response;
import io.milvus.bulkwriter.response.v2.ListImportJobsV2Response;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class CloudImportV2 extends BaseCloudImport {
    private static final Gson GSON_INSTANCE = new Gson();

    public static BulkImportV2Response createImportJobs(String url, String apiKey, BulkImportV2Request request) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v2/vectordb/jobs/import/create";
        } else {
            requestURL = String.format("https://%s/v2/vectordb/jobs/import/create", url);
        }

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<BulkImportV2Response> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<BulkImportV2Response>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData();
    }

    public static GetImportProgressV2Response getImportJobProgress(String url, String apiKey, GetImportProgressV2Request request) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v2/vectordb/jobs/import/getProgress";
        } else {
            requestURL = String.format("https://%s/v2/vectordb/jobs/import/getProgress", url);
        }

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<GetImportProgressV2Response> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<GetImportProgressV2Response>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData();
    }

    public static ListImportJobsV2Response listImportJobs(String url, String apiKey, ListImportJobsV2Request request) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v2/vectordb/jobs/import/list";
        } else {
            requestURL = String.format("https://%s/v2/vectordb/jobs/import/list", url);
        }

        Map<String, Object> params = GSON_INSTANCE.fromJson(GSON_INSTANCE.toJson(request), new TypeToken<Map<String, Object>>() {}.getType());


        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<ListImportJobsV2Response> response = GSON_INSTANCE.fromJson(body, new TypeToken<RestfulResponse<ListImportJobsV2Response>>(){}.getType());
        handleResponse(requestURL, response);
        return response.getData();
    }
}
