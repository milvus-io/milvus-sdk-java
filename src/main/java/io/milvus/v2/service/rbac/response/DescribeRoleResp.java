package io.milvus.v2.service.rbac.response;

import io.milvus.grpc.GrantEntity;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class DescribeRoleResp {
    List<GrantInfo> grantInfos;

    @Data
    @SuperBuilder
    public static class GrantInfo {
        private String objectType;
        private String privilege;
        private String objectName;
        private String dbName;
        private String grantor;
    }
}
