package io.milvus.common.resourcegroup;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.milvus.grpc.KeyValuePair;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroupNodeFilter {
    private final Map<String, String> nodeLabels;

    private ResourceGroupNodeFilter(Builder builder) {
        this.nodeLabels = builder.nodeLabels;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String, String> nodeLabels;
        private Builder() {
        }

        /**
         * Set the node label filter
         * @param key label name
         * @param value label value
         * @return <code>Builder</code>
         */
        public Builder withNodeLabel(@NonNull String key, @NonNull String value) {
            this.nodeLabels.put(key, value);
            return this;
        }

        public ResourceGroupNodeFilter build() {
            return new ResourceGroupNodeFilter(this);
        }
    }

    /**
     * Transfer to grpc
     * @return io.milvus.grpc.ResourceGroupNodeFilter
     */
    public @NonNull io.milvus.grpc.ResourceGroupNodeFilter toGRPC() {
        List<KeyValuePair> pair = ParamUtils.AssembleKvPair(nodeLabels);
        return io.milvus.grpc.ResourceGroupNodeFilter.newBuilder()
                .addAllNodeLabels(pair)
               .build();
    }

    /**
     * Constructor from grpc
     * @param filter grpc filter object
     */
    public ResourceGroupNodeFilter(io.milvus.grpc.ResourceGroupNodeFilter filter) {
        this.nodeLabels = filter.getNodeLabelsList().stream().collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
    }

}