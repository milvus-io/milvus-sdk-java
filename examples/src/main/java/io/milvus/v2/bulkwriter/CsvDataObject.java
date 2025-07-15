package io.milvus.v2.bulkwriter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.List;

public class CsvDataObject {
    private static final Gson GSON_INSTANCE = new Gson();

    @JsonProperty
    private String vector;
    @JsonProperty
    private String path;
    @JsonProperty
    private String label;

    public String getVector() {
        return vector;
    }
    public String getPath() {
        return path;
    }
    public String getLabel() {
        return label;
    }
    public List<Float> toFloatArray() {
        return GSON_INSTANCE.fromJson(vector, new TypeToken<List<Float>>() {
        }.getType());
    }
}
