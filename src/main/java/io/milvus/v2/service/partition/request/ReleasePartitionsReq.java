package io.milvus.v2.service.partition.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class ReleasePartitionsReq {
    private String collectionName;
    private List<String> partitionNames;
}
