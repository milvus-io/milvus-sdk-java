package io.milvus.v2.service.utility.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class ListBulkInsertTasksReq {
    private String collectionName;
    @Builder.Default
    private Long limit = 0L;
}
