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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.cdc.request.CrossClusterTopology;
import io.milvus.v2.service.cdc.request.DumpMessagesReq;
import io.milvus.v2.service.cdc.request.GetReplicateInfoReq;
import io.milvus.v2.service.cdc.request.MilvusCluster;
import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import io.milvus.v2.service.cdc.response.DumpMessageInfo;
import io.milvus.v2.service.cdc.response.DumpMessagesResp;
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import io.milvus.v2.service.cdc.response.GetReplicateInfoResp;

import java.util.ArrayList;
import java.util.List;

public class CDCExample {
    private static final String clusterAURI = "http://localhost:19530";
    private static final String clusterBURI = "http://localhost:29530";

    private static final String clusterAId = "cdc-a";
    private static final String clusterBId = "cdc-b";

    private static final Integer pchannelNum = 16;

    private static List<String> generatePChannels(String clusterId) {
        List<String> pchannels = new ArrayList<>(pchannelNum);
        for (int i = 0; i < pchannelNum; i++) {
            pchannels.add(clusterId + "-rootcoord-dml_" + i);
        }
        return pchannels;
    }

    public static void main(String[] args) {
        ConnectConfig clusterA = ConnectConfig.builder()
                .uri(clusterAURI)
                .build();
        MilvusClientV2 clusterAClient = new MilvusClientV2(clusterA);
        System.out.println("Cluster A connected: " + clusterAClient.getServerVersion());

        ConnectConfig clusterB = ConnectConfig.builder()
                .uri(clusterBURI)
                .build();
        MilvusClientV2 clusterBClient = new MilvusClientV2(clusterB);
        System.out.println("Cluster B connected: " + clusterBClient.getServerVersion());

        MilvusCluster milvusClusterA = MilvusCluster.builder()
                .clusterId(clusterAId)
                .uri(clusterAURI)
                .pchannels(generatePChannels(clusterAId))
                .build();
        MilvusCluster milvusClusterB = MilvusCluster.builder()
                .clusterId(clusterBId)
                .uri(clusterBURI)
                .pchannels(generatePChannels(clusterBId))
                .build();

        CrossClusterTopology topology = CrossClusterTopology.builder()
                .sourceClusterId(clusterAId)
                .targetClusterId(clusterBId)
                .build();

        ReplicateConfiguration configuration = ReplicateConfiguration.builder()
                .clusters(new ArrayList<MilvusCluster>() {{
                    add(milvusClusterA);
                    add(milvusClusterB);
                }})
                .crossClusterTopologies(new ArrayList<CrossClusterTopology>() {{
                    add(topology);
                }})
                .build();

        UpdateReplicateConfigurationReq updateReq = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .build();

        clusterAClient.updateReplicateConfiguration(updateReq);
        System.out.println("Replicate configuration updated for cluster A");
        clusterBClient.updateReplicateConfiguration(updateReq);
        System.out.println("Replicate configuration updated for cluster B");

        GetReplicateConfigurationResp replicateConfigurationResp = clusterAClient.getReplicateConfiguration();
        System.out.println("Replicate configuration: " + replicateConfigurationResp.getReplicateConfiguration());

        GetReplicateInfoResp replicateInfoResp = clusterBClient.getReplicateInfo(GetReplicateInfoReq.builder()
                .sourceClusterId(clusterAId)
                .targetPchannel(generatePChannels(clusterBId).get(0))
                .build());
        System.out.println("Replicate info: " + replicateInfoResp);

        String dumpStartMessageId = replicateInfoResp.getSalvageCheckpoint().getMessageID().getId();

        DumpMessagesResp dumpMessagesResp = clusterAClient.dumpMessages(DumpMessagesReq.builder()
                .pchannel(generatePChannels(clusterAId).get(0))
                .startMessageID(GetReplicateInfoResp.MessageID.builder()
                        .id(dumpStartMessageId)
                        .walName("RocksMQ")
                        .build())
                .build());
        System.out.println("Dump messages:");
        for (DumpMessageInfo message : dumpMessagesResp) {
            System.out.println("\tmessage id: " + message.getMessageID().getId());
            System.out.println("\tpayload size: " + message.getPayload().length);
        }
    }
}
