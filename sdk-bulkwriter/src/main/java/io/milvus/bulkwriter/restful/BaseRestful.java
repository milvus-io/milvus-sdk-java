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

import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.ExceptionUtils;
import kong.unirest.Unirest;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class BaseRestful {
    protected static String postRequest(String url, String apiKey, Map<String, Object> params, int timeout) {
        try {
            setDefaultOptionsIfCallCloud(params, apiKey);
            kong.unirest.HttpResponse<String> response = Unirest.post(url)
                    .connectTimeout(timeout)
                    .headers(httpHeaders(apiKey))
                    .body(params).asString();
            if (response.getStatus() != 200) {
                ExceptionUtils.throwUnExpectedException(String.format("Failed to post url: %s, status code: %s, msg: %s", url, response.getStatus(), response.getBody()));
            } else {
                return response.getBody();
            }
        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to post url: %s, error: %s", url, e));
        }
        return null;
    }

    protected static String deleteRequest(String url, String apiKey, Map<String, Object> params, int timeout) {
        try {
            setDefaultOptionsIfCallCloud(params, apiKey);
            kong.unirest.HttpResponse<String> response = Unirest.delete(url)
                    .connectTimeout(timeout)
                    .headers(httpHeaders(apiKey))
                    .asString();
            if (response.getStatus() != 200) {
                ExceptionUtils.throwUnExpectedException(String.format("Failed to delete url: %s, status code: %s, msg: %s", url, response.getStatus(), response.getBody()));
            } else {
                return response.getBody();
            }
        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to delete url: %s, error: %s", url, e));
        }
        return null;
    }

    protected static String getRequest(String url, String apiKey, Map<String, Object> params, int timeout) {
        try {
            kong.unirest.HttpResponse<String> response = Unirest.get(url)
                    .connectTimeout(timeout)
                    .headers(httpHeaders(apiKey))
                    .queryString(params).asString();
            if (response.getStatus() != 200) {
                ExceptionUtils.throwUnExpectedException(String.format("Failed to get url: %s, status code: %s, msg: %s", url, response.getStatus(), response.getBody()));
            } else {
                return response.getBody();
            }
        } catch (Exception e) {
            ExceptionUtils.throwUnExpectedException(String.format("Failed to get url: %s, error: %s", url, e));
        }
        return null;
    }


    protected static Map<String, String> httpHeaders(String apiKey) {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) " +
                "Chrome/17.0.963.56 Safari/535.11");
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        header.put("Accept-Encodin", "gzip,deflate,sdch");
        header.put("Accept-Languag", "en-US,en;q=0.5");
        if (StringUtils.isNotEmpty(apiKey)) {
            header.put("Authorization", "Bearer " + apiKey);
        }

        return header;
    }

    protected static void handleResponse(String url, RestfulResponse res) {
        int innerCode = res.getCode();
        if (innerCode != 0) {
            String innerMessage = res.getMessage();
            ExceptionUtils.throwUnExpectedException(String.format("Failed to request url: %s, code: %s, message: %s", url, innerCode, innerMessage));
        }
    }

    private static void setDefaultOptionsIfCallCloud(Map<String, Object> params, String apiKey) {
        if (StringUtils.isNotEmpty(apiKey)) {
            Map<String, Object> options = new HashMap<>();
            options.put("sdk", "java");
            options.put("scene", "bulkWriter");

            params.put("options", options);
        }
    }
}
