package io.milvus.v2.service.utility.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class ListBulkInsertTasksResp {
    private List<GetBulkInsertStateResp> tasks;
}
