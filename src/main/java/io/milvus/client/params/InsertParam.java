package io.milvus.client.params;

import java.util.ArrayList;
import java.util.List;

public class InsertParam {
    private final String tableName;
    private final List<List<Float>> vectors;
    private final List<Long> vectorIds;

    public static class Builder {
        // Required parameters
        private final String tableName;
        private final List<List<Float>> vectors;

        // Optional parameters - initialized to default values
        private List<Long> vectorIds = new ArrayList<>();

        public Builder(String tableName, List<List<Float>> vectors) {
            this.tableName = tableName;
            this.vectors = vectors;
        }

        public Builder setVectorIds(List<Long> val) {
            vectorIds = val;
            return this;
        }

        public InsertParam build() {
            return new InsertParam(this);
        }
    }

    private InsertParam(Builder builder) {
        this.tableName = builder.tableName;
        this.vectors = builder.vectors;
        this.vectorIds = builder.vectorIds;
    }

    public String getTableName() {
        return tableName;
    }

    public List<List<Float>> getVectors() {
        return vectors;
    }

    public List<Long> getVectorIds() {
        return vectorIds;
    }
}
