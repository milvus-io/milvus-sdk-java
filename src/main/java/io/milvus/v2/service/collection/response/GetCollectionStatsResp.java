package io.milvus.v2.service.collection.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class GetCollectionStatsResp {
    private Long numOfEntities;
}
