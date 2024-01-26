package io.milvus.v2.service.rbac.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeRoleReq {
    private String roleName;
}
