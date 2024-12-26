package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder
public class UpdateResourceGroupsReq {
    @Builder.Default
    private Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
}
