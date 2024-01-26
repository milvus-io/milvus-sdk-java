package io.milvus.v2.service.rbac.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class GrantPrivilegeReq {
    private String roleName;
    private String objectType;
    private String privilege;
    private String objectName;
}
