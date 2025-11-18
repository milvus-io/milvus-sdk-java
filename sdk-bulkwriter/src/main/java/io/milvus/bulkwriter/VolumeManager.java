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
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.response.volume.ListVolumesResponse;
import io.milvus.bulkwriter.restful.DataVolumeUtils;

public class VolumeManager {
    private final String cloudEndpoint;
    private final String apiKey;

    public VolumeManager(VolumeManagerParam volumeManagerParam) {
        cloudEndpoint = volumeManagerParam.getCloudEndpoint();
        apiKey = volumeManagerParam.getApiKey();
    }

    /**
     * Create a volume under the specified project and regionId.
     */
    public void createVolume(CreateVolumeRequest request) {
        DataVolumeUtils.createVolume(cloudEndpoint, apiKey, request);
    }

    /**
     * Delete a volume.
     */
    public void deleteVolume(DeleteVolumeRequest request) {
        DataVolumeUtils.deleteVolume(cloudEndpoint, apiKey, request);
    }

    /**
     * Paginated query of the volume list under a specified projectId.
     */
    public ListVolumesResponse listVolumes(ListVolumesRequest request) {
        String result = DataVolumeUtils.listVolumes(cloudEndpoint, apiKey, request);
        return new Gson().fromJson(result, ListVolumesResponse.class);
    }
}