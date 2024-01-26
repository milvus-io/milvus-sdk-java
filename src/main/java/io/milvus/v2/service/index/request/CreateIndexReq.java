package io.milvus.v2.service.index.request;

import io.milvus.v2.common.IndexParam;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class CreateIndexReq {
    private String collectionName;
    private IndexParam indexParam;
}
