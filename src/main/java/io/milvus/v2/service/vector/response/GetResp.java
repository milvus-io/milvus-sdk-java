package io.milvus.v2.service.vector.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class GetResp {
    public List<QueryResp.QueryResult> getResults;
}
