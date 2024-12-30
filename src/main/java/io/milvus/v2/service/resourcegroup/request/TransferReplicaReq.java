package io.milvus.v2.service.resourcegroup.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class TransferReplicaReq {
    private String sourceGroupName;
    private String targetGroupName;
    private String collectionName;
    private String databaseName;
    private Long numberOfReplicas;
}
