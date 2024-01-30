package io.milvus.v2.service.index.request;

import io.milvus.v2.common.IndexParam;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class CreateIndexReq {
    private String collectionName;
    private List<IndexParam> indexParams;
}
