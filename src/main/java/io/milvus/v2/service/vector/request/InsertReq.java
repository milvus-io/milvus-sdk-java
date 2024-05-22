package io.milvus.v2.service.vector.request;

import com.google.gson.JsonObject;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class InsertReq {
    //private List<> fields;
    private List<JsonObject> data;
    private String collectionName;
    @Builder.Default
    private String partitionName = "";
}
