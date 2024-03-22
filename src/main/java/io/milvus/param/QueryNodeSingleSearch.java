package io.milvus.param;

import io.milvus.exception.ParamException;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Defined single search for query node listener send heartbeat.
 */
public class QueryNodeSingleSearch {

    private final String collectionName;
    private final MetricType metricType;
    private final String vectorFieldName;
    private final List<?> vectors;
    private final String params;

    private QueryNodeSingleSearch(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.vectors = builder.vectors;
        this.params = builder.params;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public List<?> getVectors() {
        return vectors;
    }

    public String getParams() {
        return params;
    }

    /**
     * Builder for {@link QueryNodeSingleSearch}
     */
    public static class Builder {
        private String collectionName;
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private List<?> vectors;
        private String params = "{}";

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets metric type of ANN searching.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets target vector field by name. Field name cannot be empty or null.
         *
         * @param vectorFieldName vector field name
         * @return <code>Builder</code>
         */
        public Builder withVectorFieldName(@NonNull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets the target vectors.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float
         *                if vector type is BinaryVector/Float16Vector/BFloat16Vector, vectors is List of ByteBuffer
         * @return <code>Builder</code>
         */
        public Builder withVectors(@NonNull List<?> vectors) {
            this.vectors = vectors;
            return this;
        }

        /**
         * Sets the search parameters specific to the index type.
         *
         * For example: IVF index, the search parameters can be "{\"nprobe\":10}"
         * For more information: @see <a href="https://milvus.io/docs/v2.0.0/index_selection.md">Index Selection</a>
         *
         * @param params extra parameters in json format
         * @return <code>Builder</code>
         */
        public Builder withParams(@NonNull String params) {
            this.params = params;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link QueryNodeSingleSearch} instance.
         *
         * @return {@link QueryNodeSingleSearch}
         */
        public QueryNodeSingleSearch build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (vectors == null || vectors.isEmpty()) {
                throw new ParamException("Target vectors can not be empty");
            }

            if (vectors.get(0) instanceof List) {
                // float vectors
                List<?> first = (List<?>) vectors.get(0);
                if (!(first.get(0) instanceof Float)) {
                    throw new ParamException("Float vector field's value must be List<Float>");
                }

                int dim = first.size();
                for (int i = 1; i < vectors.size(); ++i) {
                    List<?> temp = (List<?>) vectors.get(i);
                    if (dim != temp.size()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else if (vectors.get(0) instanceof ByteBuffer) {
                // binary vectors
                ByteBuffer first = (ByteBuffer) vectors.get(0);
                int dim = first.position();
                for (int i = 1; i < vectors.size(); ++i) {
                    ByteBuffer temp = (ByteBuffer) vectors.get(i);
                    if (dim != temp.position()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else {
                throw new ParamException("Target vector type must be List<Float> or ByteBuffer");
            }

            return new QueryNodeSingleSearch(this);
        }
    }
}
