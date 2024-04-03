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
import io.milvus.common.utils.ExceptionUtils;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class CloudImport {
    public static BulkImportResponse bulkImport(String url, String apiKey, String objectUrl,
                                                String accessKey, String secretKey, String clusterId, String collectionName) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v1/vector/collections/import";
        } else {
            requestURL = String.format("https://%s/v1/vector/collections/import", url);
        }

        Map<String, Object> params = new HashMap<>();
        params.put("objectUrl", objectUrl);
        params.put("accessKey", accessKey);
        params.put("secretKey", secretKey);
        params.put("clusterId", clusterId);
        params.put("collectionName", collectionName);

        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<BulkImportResponse> response = new Gson().fromJson(body, new TypeToken<RestfulResponse<BulkImportResponse>>(){}.getType());
        handleResponse(url, response);
        return response.getData();
    }

    public static GetImportProgressResponse getImportProgress(String url, String apiKey, String jobId, String clusterId) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v1/vector/collections/import/get";
        } else {
            requestURL = String.format("https://%s/v1/vector/collections/import/get", url);
        }

        Map<String, Object> params = new HashMap<>();
        params.put("clusterId", clusterId);
        params.put("jobId", jobId);

        String body = getRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<GetImportProgressResponse> response = new Gson().fromJson(body, new TypeToken<RestfulResponse<GetImportProgressResponse>>(){}.getType());
        handleResponse(url, response);
        return response.getData();
    }

    public static ListImportJobsResponse listImportJobs(String url, String apiKey, String clusterId, int pageSize, int currentPage) throws MalformedURLException {
        String requestURL;
        String protocol = new URL(url).getProtocol();
        if (protocol.startsWith("http")) {
            requestURL = url + "/v1/vector/collections/import/list";
        } else {
            requestURL = String.format("https://%s/v1/vector/collections/import/list", url);
        }

        Map<String, Object> params = new HashMap<>();
        params.put("clusterId", clusterId);
        params.put("pageSize", pageSize);
        params.put("currentPage", currentPage);

        String body = getRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<ListImportJobsResponse> response = new Gson().fromJson(body, new TypeToken<RestfulResponse<ListImportJobsResponse>>(){}.getType());
        handleResponse(url, response);
        return response.getData();
    }

    private static String postRequest(String url, String apiKey, Map<String, Object> params, int timeout) {
        try {
            kong.unirest.HttpResponse<String> response = Unirest.post(url)
                    .connectTimeout(timeout)
                    .headers(httpHeaders(apiKey))
                    .body(params).asString();
            if (response.getStatus() != 200) {
                ExceptionUtils.throwUnExpectedException(String.format("Failed to post url: %s, status code: %s", url, response.getStatus()));
            } else {
                return response.getBody();
            }
        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to post url: %s, error: %s", url, e));
        }
        return null;
    }

    private static String getRequest(String url, String apiKey, Map<String, Object> params, int timeout) {
        try {
            kong.unirest.HttpResponse<String> response = Unirest.get(url)
                    .connectTimeout(timeout)
                    .headers(httpHeaders(apiKey))
                    .queryString(params).asString();
            if (response.getStatus() != 200) {
                ExceptionUtils.throwUnExpectedException(String.format("Failed to get url: %s, status code: %s", url, response.getStatus()));
            } else {
                return response.getBody();
            }
        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to get url: %s, error: %s", url, e));
        }
        return null;
    }


    private static Map<String, String> httpHeaders(String apiKey) {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) " +
                "Chrome/17.0.963.56 Safari/535.11");
        header.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
        header.put("Accept-Encodin", "gzip,deflate,sdch");
        header.put("Accept-Languag", "en-US,en;q=0.5");
        header.put("Authorization", "Bearer " + apiKey);

        return header;
    }

    private static void handleResponse(String url, RestfulResponse res) {
        int innerCode = res.getCode();
        if (innerCode != 200) {
            String innerMessage = res.getMessage();
            ExceptionUtils.throwUnExpectedException(String.format("Failed to request url: %s, code: %s, message: %s", url, innerCode, innerMessage));
        }
    }
}
