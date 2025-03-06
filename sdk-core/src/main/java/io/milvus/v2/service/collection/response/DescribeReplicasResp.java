package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.ReplicaInfo;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class DescribeReplicasResp {
    @Builder.Default
    private List<ReplicaInfo> replicas = new ArrayList<>();
}
