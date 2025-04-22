package io.milvus.v2.service.resourcegroup.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class TransferNodeReq {
    String sourceGroupName;
    String targetGroupName;
    Integer numOfNodes;
}
