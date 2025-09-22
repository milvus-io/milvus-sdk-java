package io.milvus.v2.service.utility.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class GetQuerySegmentInfoReq {
    private String databaseName;
    private String collectionName;
}
