package io.milvus.v2.service.collection.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class LoadCollectionReq {
    private String collectionName;
    private List<String> partitionNames;
}
