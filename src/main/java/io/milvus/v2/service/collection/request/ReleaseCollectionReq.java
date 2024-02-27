package io.milvus.v2.service.collection.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class ReleaseCollectionReq {
    private String collectionName;
}
