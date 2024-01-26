package io.milvus.v2.service.collection.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class ListCollectionsResp {
    private List<String> collectionNames;
}
