package io.milvus.v2.service.vector.request.ranker;

import com.google.gson.JsonArray;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Model reranking strategy, which transforms Milvus search by integrating advanced language models
 * that understand semantic relationships between queries and documents.
 * Read the doc for more info: https://milvus.io/docs/model-ranker-overview.md
 *
 * You also can declare a model ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .name("semantic_ranker")
 *                 .description("semantic ranker")
 *                 .inputFieldNames(Collections.singletonList("document"))
 *                 .param("reranker", "model")
 *                 .param("provider", "tei")
 *                 .param("queries", "[\"machine learning for time series\"]")
 *                 .param("endpoint", "http://model-service:8080")
 *                 .build();
 */
public class ModelRanker extends CreateCollectionReq.Function {
    private String provider;
    private List<String> queries;
    private String endpoint;

    private ModelRanker(FunctionBuilder builder) {
        super(builder);
        this.provider = builder.provider;
        this.queries = builder.queries;
        this.endpoint = builder.endpoint;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    @Override
    public Map<String, String> getParams() {
        // the parent params might contain "offset" and "decay"
        Map<String, String> props = super.getParams();
        props.put("reranker", "model");
        props.put("provider", provider); // "tei" or "vllm"
        JsonArray json = new JsonArray();
        queries.forEach(json::add);
        props.put("queries", json.toString());
        if (endpoint != null) {
            props.put("endpoint", endpoint);
        }
        return props;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        ModelRanker that = (ModelRanker) obj;
        return new EqualsBuilder()
                .append(provider, that.provider)
                .append(queries, that.queries)
                .append(endpoint, that.endpoint)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (provider != null ? provider.hashCode() : 0);
        result = 31 * result + (queries != null ? queries.hashCode() : 0);
        result = 31 * result + (endpoint != null ? endpoint.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ModelRanker{" +
                "provider='" + provider + '\'' +
                ", queries=" + queries +
                ", endpoint='" + endpoint + '\'' +
                ", name='" + getName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", functionType=" + getFunctionType() +
                ", inputFieldNames=" + getInputFieldNames() +
                ", outputFieldNames=" + getOutputFieldNames() +
                ", params=" + getParams() +
                '}';
    }

    public static FunctionBuilder builder() {
        return new FunctionBuilder();
    }

    public static class FunctionBuilder extends Function.FunctionBuilder {
        private String provider = "tei";
        private List<String> queries = new ArrayList<>();
        private String endpoint;

        private FunctionBuilder() {}

        public FunctionBuilder provider(String provider) {
            this.provider = provider;
            return this;
        }

        public FunctionBuilder queries(List<String> queries) {
            this.queries = queries;
            return this;
        }

        public FunctionBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        @Override
        public FunctionBuilder name(String name) {
            super.name(name);
            return this;
        }

        @Override
        public FunctionBuilder description(String description) {
            super.description(description);
            return this;
        }

        @Override
        public FunctionBuilder functionType(io.milvus.common.clientenum.FunctionType functionType) {
            super.functionType(functionType);
            return this;
        }

        @Override
        public FunctionBuilder inputFieldNames(java.util.List<String> inputFieldNames) {
            super.inputFieldNames(inputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder outputFieldNames(java.util.List<String> outputFieldNames) {
            super.outputFieldNames(outputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder params(java.util.Map<String, String> params) {
            super.params(params);
            return this;
        }

        @Override
        public FunctionBuilder param(String key, String value) {
            super.param(key, value);
            return this;
        }

        public ModelRanker build() {
            return new ModelRanker(this);
        }
    }
}
