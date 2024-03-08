package io.milvus.v2.service.vector.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class QueryResp {
    private List<QueryResult> queryResults;

    @Data
    @SuperBuilder
    public static class QueryResult {
        private Map<String, Object> entity;
    }
}
