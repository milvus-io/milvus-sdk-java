package io.milvus.v2.service.index.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder
@Deprecated
public class AlterIndexReq {
    private String collectionName;
    private String databaseName;
    private String indexName;
    @Builder.Default
    private Map<String, String> properties = new HashMap<>();
}
