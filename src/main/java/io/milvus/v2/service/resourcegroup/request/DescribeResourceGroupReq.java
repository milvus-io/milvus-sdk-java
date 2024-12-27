package io.milvus.v2.service.resourcegroup.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeResourceGroupReq {
    private String groupName;
}
