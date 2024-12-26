package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class CreateResourceGroupReq {
    private String groupName;
    private ResourceGroupConfig config;
}
