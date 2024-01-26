package io.milvus.v2.service.utility.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class ListAliasResp {
    private String collectionName;
    private List<String> alias;
}
