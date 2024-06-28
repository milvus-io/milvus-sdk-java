package io.milvus.v2.service.collection.request;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder
public class AlterCollectionReq {
    private String collectionName;
    private String databaseName;
    @Builder.Default
    private final Map<String, String> properties = new HashMap<>();
}
