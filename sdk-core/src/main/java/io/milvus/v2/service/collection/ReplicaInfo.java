package io.milvus.v2.service.collection;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class ReplicaInfo {
    private Long replicaID;
    private Long collectionID;
    @Builder.Default
    private List<Long> partitionIDs = new ArrayList<>();
    @Builder.Default
    private List<ShardReplica> shardReplicas = new ArrayList<>();
    @Builder.Default
    private List<Long> nodeIDs = new ArrayList<>(); // include leaders
    @Builder.Default
    private String resourceGroupName = "";
    @Builder.Default
    private Map<String, Integer> numOutboundNode = new HashMap<>();
}
