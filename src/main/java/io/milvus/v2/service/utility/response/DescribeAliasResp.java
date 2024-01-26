package io.milvus.v2.service.utility.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class DescribeAliasResp {
    private String collectionName;
    private String alias;
}
