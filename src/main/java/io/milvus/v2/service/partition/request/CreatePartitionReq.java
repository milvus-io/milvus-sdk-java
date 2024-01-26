package io.milvus.v2.service.partition.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class CreatePartitionReq {
    private String collectionName;
    private String partitionName;
}
