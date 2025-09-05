package io.milvus.v2.service.collection;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class CollectionInfo {
    private String collectionName;
    private Integer shardNum;
}
