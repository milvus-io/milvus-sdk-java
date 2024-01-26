package io.milvus.v2.service.vector.request;

import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class UpsertReq {
    private List<Map<String, Object>> upsertData;
    private String collectionName;
    @Builder.Default
    private String partitionName = "";

    public List<JSONObject> getUpsertData() {
        return new ArrayList<JSONObject>();
    }
}
