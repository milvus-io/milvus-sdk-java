package io.milvus.v2.service.utility.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class BulkInsertResp {
    @Builder.Default
    private List<Long> tasks = new ArrayList<>();
}
