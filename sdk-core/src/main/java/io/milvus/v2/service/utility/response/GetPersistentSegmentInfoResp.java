package io.milvus.v2.service.utility.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@Data
@SuperBuilder
public class GetPersistentSegmentInfoResp {
    @Data
    @SuperBuilder
    public static class PersistentSegmentInfo {
        private Long segmentID;
        private Long collectionID;
        private Long partitionID;
        private Long numOfRows;
        private String state;
        private String level;
        private Boolean isSorted;
    }

    @Builder.Default
    private List<PersistentSegmentInfo> segmentInfos = new ArrayList<>();
}
