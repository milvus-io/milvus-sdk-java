package io.milvus.v2.service.collection;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class ShardReplica {
    private Long leaderID;
    private String leaderAddress; // IP:port
    @Builder.Default
    private String channelName = "";
    @Builder.Default
    private List<Long> nodeIDs = new ArrayList<>();
}
