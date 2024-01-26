package io.milvus.v2.service.vector.response;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class SearchResp {
    private List<SearchResult> searchResults;

    @Data
    @SuperBuilder
    public static class SearchResult {
        private Map<String, Object> fields;
        private Float score;
    }
}
