package io.milvus.v2.service.vector.request.ranker;

import com.google.gson.JsonArray;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

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
@SuperBuilder
public class ModelRanker extends CreateCollectionReq.Function {
    @Builder.Default
    private String provider = "tei";
    @Builder.Default
    private List<String> queries = new ArrayList<>();
    private String endpoint;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

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
}
