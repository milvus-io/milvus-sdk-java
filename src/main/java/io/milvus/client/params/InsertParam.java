package io.milvus.client.params;

import javax.annotation.Nonnull;
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

        public Builder withVectorIds(List<Long> vectorIds) {
            this.vectorIds = vectorIds;
            return this;
        }

        public InsertParam build() {
            return new InsertParam(this);
        }
    }

    private InsertParam(@Nonnull Builder builder) {
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
