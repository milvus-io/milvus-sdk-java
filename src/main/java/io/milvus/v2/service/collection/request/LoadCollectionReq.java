package io.milvus.v2.service.collection.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class LoadCollectionReq {
    private String collectionName;
    @Builder.Default
    private Integer replicaNum = 1;
    @Builder.Default
    private Boolean async = Boolean.TRUE;
    @Builder.Default
    private Long timeout = 60000L;
}
