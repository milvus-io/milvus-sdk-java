package io.milvus.v2.service.rbac.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class DescribeUserResp {
    private List<String> roles;
}
