package io.milvus.v2.service.utility.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class BulkInsertReq {
    private String collectionName;
    @Builder.Default
    private String partitionName = "";
    @Builder.Default
    private List<String> files = new ArrayList<>();
}
