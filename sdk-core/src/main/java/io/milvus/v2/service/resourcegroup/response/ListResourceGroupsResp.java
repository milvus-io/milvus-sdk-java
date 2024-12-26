package io.milvus.v2.service.resourcegroup.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class ListResourceGroupsResp {
    @Builder.Default
    private List<String> groupNames = new ArrayList<>();
}
