package io.milvus.v2.service.vector.request.ranker;

import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/**
 * The Decay reranking strategy, which by adjusting search rankings based on numeric field values.
 * Read the doc for more info: https://milvus.io/docs/decay-ranker-overview.md
 *
 * You also can declare a decay ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .name("time_decay")
 *                 .description("time decay")
 *                 .inputFieldNames(Collections.singletonList("timestamp"))
 *                 .param("reranker", "decay")
 *                 .param("function", "gauss")
 *                 .param("origin", "1000")
 *                 .param("scale", "10000")
 *                 .param("offset", "24")
 *                 .param("decay", "0.5")
 *                 .build();
 */
@SuperBuilder
public class DecayRanker extends CreateCollectionReq.Function {
    @Builder.Default
    private String function = "gauss";
    private Number origin;
    private Number scale;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    public Map<String, String> getParams() {
        // the parent params might contain "offset" and "decay"
        Map<String, String> props = super.getParams();
        props.put("reranker", "decay");
        props.put("function", function); // "gauss", "exp", or "linear"
        if (origin != null) {
            props.put("origin", origin.toString());
        }
        if (scale != null) {
            props.put("scale", scale.toString());
        }
        return props;
    }
}
