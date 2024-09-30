package io.milvus.v2.service.utility.response;

import io.milvus.v2.common.BulkInsertState;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class GetBulkInsertStateResp {
    private Long taskID;
    private Long collectionID;
    private List<Long> segmentIDs;
    private BulkInsertState state;
    private Long rowCount;
    private List<Long> autoGenIDRange;
    private Map<String, String> infos;
}
