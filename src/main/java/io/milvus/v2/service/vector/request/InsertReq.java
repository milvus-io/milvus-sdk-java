package io.milvus.v2.service.vector.request;

import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@SuperBuilder
public class InsertReq {
    //private List<> fields;
    private List<JSONObject> insertData;
    private String collectionName;
    @Builder.Default
    private String partitionName = "";
}
