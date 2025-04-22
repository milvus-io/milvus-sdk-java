package io.milvus.v2.service.utility.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class GetQuerySegmentInfoResp {
    @Data
    @SuperBuilder
    public static class QuerySegmentInfo {
        private Long segmentID;
        private Long collectionID;
        private Long partitionID;
        private Long memSize;
        private Long numOfRows;
        private String indexName;
        private Long indexID;
        private String state;
        private String level;
        @Builder.Default
        private List<Long> nodeIDs = new ArrayList<>();
        private Boolean isSorted;
    }

    @Builder.Default
    private List<QuerySegmentInfo> segmentInfos = new ArrayList<>();
}
