package io.milvus.v2.service.rbac.request;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class RevokePrivilegeReq {
    private String roleName;
    private String dbName;
    private String objectType;
    private String privilege;
    private String objectName;
}
