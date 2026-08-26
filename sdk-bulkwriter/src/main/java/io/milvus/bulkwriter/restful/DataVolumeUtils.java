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
import io.milvus.bulkwriter.request.volume.BaseVolumeRequest;
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.DescribeVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * REST helper for managing Milvus data volumes used by the bulk writer.
 *
 * <p>Provides methods to apply, list, create, describe, and delete data volumes through the
 * Milvus {@code /v2/volumes} REST API. Volume operations let the bulk writer allocate and manage
 * the remote storage volume that holds the uploaded data files.
 */
public class DataVolumeUtils extends BaseRestful {
    /**
     * Applies for a data volume.
     *
     * @param url the Milvus REST API base URL
     * @param request the apply-volume request
     * @return the JSON string of the applied volume data
     */
    public static String applyVolume(String url, BaseVolumeRequest request) {
        String requestURL = url + "/v2/volumes/apply";

        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
        }.getType());
        String body = postRequest(requestURL, request.getApiKey(), params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
        return new Gson().toJson(response.getData());
    }

    /**
     * Lists the data volumes visible to the given API key.
     *
     * @param url the Milvus REST API base URL
     * @param apiKey the API key used for authentication
     * @param request the list-volumes request
     * @return the JSON string of the listed volumes data
     */
    public static String listVolumes(String url, String apiKey, ListVolumesRequest request) {
        String requestURL = url + "/v2/volumes";

        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
        }.getType());
        String body = getRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
        return new Gson().toJson(response.getData());
    }

    /**
     * Creates a data volume.
     *
     * @param url the Milvus REST API base URL
     * @param apiKey the API key used for authentication
     * @param request the create-volume request
     */
    public static void createVolume(String url, String apiKey, CreateVolumeRequest request) {
        String requestURL = url + "/v2/volumes/create";

        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
        }.getType());
        String body = postRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
    }

    /**
     * Describes the data volume with the given name.
     *
     * @param url the Milvus REST API base URL
     * @param apiKey the API key used for authentication
     * @param request the describe-volume request
     * @return the JSON string of the volume data
     */
    public static String describeVolume(String url, String apiKey, DescribeVolumeRequest request) {
        String requestURL = url + "/v2/volumes/" + request.getVolumeName();

        Map<String, Object> params = new HashMap<>();
        String body = getRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
        return new Gson().toJson(response.getData());
    }

    /**
     * Deletes the data volume with the given name.
     *
     * @param url the Milvus REST API base URL
     * @param apiKey the API key used for authentication
     * @param request the delete-volume request
     */
    public static void deleteVolume(String url, String apiKey, DeleteVolumeRequest request) {
        String requestURL = url + "/v2/volumes/" + request.getVolumeName();

        Map<String, Object> params = JsonUtils.fromJson(JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
        }.getType());
        String body = deleteRequest(requestURL, apiKey, params, 60 * 1000);
        RestfulResponse<Object> response = JsonUtils.fromJson(body, new TypeToken<RestfulResponse<Object>>() {
        }.getType());
        handleResponse(requestURL, response);
    }
}
