package io.milvus.response;

import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.DescribeIndexResponse;

import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;

/**
 * Util class to wrap response of <code>describeIndex</code> interface.
 */
public class DescIndexResponseWrapper {
    private final DescribeIndexResponse response;

    public DescIndexResponseWrapper(@NonNull DescribeIndexResponse response) {
        this.response = response;
    }

    /**
     * Get index description of fields.
     *
     * @return List of IndexDesc, index description of fields
     */
    public List<IndexDesc> getIndexDescriptions() {
        List<IndexDesc> results = new ArrayList<>();
        List<IndexDescription> descriptions = response.getIndexDescriptionsList();
        descriptions.forEach((desc)->{
            IndexDesc res = new IndexDesc(desc.getFieldName(), desc.getIndexName(), desc.getIndexID());
            desc.getParamsList().forEach((kv)-> res.addParam(kv.getKey(), kv.getValue()));
            results.add(res);
        });

        return results;
    }

    /**
     * Get index description by field name.
     * Return null if the field doesn't exist
     *
     * @param fieldName field name to get index description
     * @return {@link IndexDesc} description of the index
     */
    public IndexDesc getIndexDescByFieldName(@NonNull String fieldName) {
        for (int i = 0; i < response.getIndexDescriptionsCount(); ++i) {
            IndexDescription desc = response.getIndexDescriptions(i);
            if (fieldName.compareTo(desc.getFieldName()) == 0) {
                IndexDesc res = new IndexDesc(desc.getFieldName(), desc.getIndexName(), desc.getIndexID());
                desc.getParamsList().forEach((kv)-> res.addParam(kv.getKey(), kv.getValue()));
                return res;
            }
        }

        return null;
    }

    /**
     * Internal-use class to wrap response of <code>describeIndex</code> interface.
     */
    @Getter
    public static final class IndexDesc {
        private final String fieldName;
        private final String indexName;
        private final long id;
        private final Map<String, String> params = new HashMap<>();

        public IndexDesc(@NonNull String fieldName, @NonNull String indexName, long id) {
            this.fieldName = fieldName;
            this.indexName = indexName;
            this.id = id;
        }

        public void addParam(@NonNull String key, @NonNull String value) {
            this.params.put(key, value);
        }

        public IndexType getIndexType() {
            if (this.params.containsKey(Constant.INDEX_TYPE)) {
                // may throw IllegalArgumentException
                return IndexType.valueOf(params.get(Constant.INDEX_TYPE).toUpperCase(Locale.ROOT));
            }

            return IndexType.None;
        }

        public MetricType getMetricType() {
            if (this.params.containsKey(Constant.METRIC_TYPE)) {
                // may throw IllegalArgumentException
                return MetricType.valueOf(params.get(Constant.METRIC_TYPE));
            }

            return MetricType.INVALID;
        }

        public String getExtraParam() {
            if (this.params.containsKey(Constant.PARAMS)) {
                // may throw IllegalArgumentException
                return params.get(Constant.PARAMS);
            }

            return "";
        }

        @Override
        public String toString() {
            return "IndexDesc(fieldName: " + getFieldName() + " indexName: " + getIndexName() +
                    " id: " + getId() + " indexType: " + getIndexType().name() + " metricType: " +
                    getMetricType().name() + " extraParam: " + getExtraParam() + ")";
        }
    }

    /**
     * Construct a <code>String</code> by {@link DescIndexResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Index description{" +
                getIndexDescriptions().toString() +
                '}';
    }
}
