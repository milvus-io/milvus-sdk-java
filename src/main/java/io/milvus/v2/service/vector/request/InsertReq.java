package io.milvus.v2.service.vector.request;

import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class InsertReq {
    //private List<> fields;
    private List<JSONObject> data;
    private String collectionName;
    @Builder.Default
    private String partitionName = "";
}
