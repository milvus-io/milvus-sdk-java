package io.milvus.v2.service.vector.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class GetReq {
    private String collectionName;
    @Builder.Default
    private String partitionName = "";
    private List<Object> ids;
    private List<String> outputFields;
}
