package io.milvus.v2.service.partition.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class HasPartitionReq {
    private String collectionName;
    private String partitionName;
}
