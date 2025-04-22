package io.milvus.v2.service.utility.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class CheckHealthResp {
    @Builder.Default
    Boolean isHealthy = false;
    @Builder.Default
    List<String> reasons = new ArrayList<>();
    @Builder.Default
    List<String> quotaStates = new ArrayList<>();
}
