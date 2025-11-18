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
package io.milvus.v2;

import com.google.gson.Gson;
import io.milvus.bulkwriter.VolumeManager;
import io.milvus.bulkwriter.VolumeManagerParam;
import io.milvus.bulkwriter.request.volume.CreateVolumeRequest;
import io.milvus.bulkwriter.request.volume.DeleteVolumeRequest;
import io.milvus.bulkwriter.request.volume.ListVolumesRequest;
import io.milvus.bulkwriter.response.volume.ListVolumesResponse;


public class VolumeManagerExample {
    private static final VolumeManager volumeManager;

    static {
        VolumeManagerParam volumeManagerParam = VolumeManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("_api_key_for_cluster_org_")
                .build();
        volumeManager = new VolumeManager(volumeManagerParam);
    }

    private static final String PROJECT_ID = "_id_for_project_";
    private static final String REGION_ID = "_id_for_region_";
    private static final String VOLUME_NAME = "_volume_name_for_project_";

    public static void main(String[] args) throws Exception {
        createVolume();
        listVolumes();
        deleteVolume();
    }

    private static void createVolume() {
        CreateVolumeRequest request = CreateVolumeRequest.builder()
                .projectId(PROJECT_ID).regionId(REGION_ID).volumeName(VOLUME_NAME)
                .build();
        volumeManager.createVolume(request);
        System.out.printf("\nVolume %s created%n", VOLUME_NAME);
    }

    private static void listVolumes() {
        ListVolumesRequest request = ListVolumesRequest.builder()
                .projectId(PROJECT_ID).currentPage(1).pageSize(10)
                .build();
        ListVolumesResponse response = volumeManager.listVolumes(request);
        System.out.println("\nlistVolumes results: " + new Gson().toJson(response));
    }

    private static void deleteVolume() {
        DeleteVolumeRequest request = DeleteVolumeRequest.builder()
                .volumeName(VOLUME_NAME)
                .build();
        volumeManager.deleteVolume(request);
        System.out.printf("\nVolume %s deleted%n", VOLUME_NAME);
    }
}
