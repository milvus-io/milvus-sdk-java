package io.milvus.Response;

import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.DescribeIndexResponse;

import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * @return <code>List<IndexDesc></code> index description of fields
     */
    public List<IndexDesc> GetIndexDescriptions() {
        List<IndexDesc> results = new ArrayList<>();
        List<IndexDescription> descriptions = response.getIndexDescriptionsList();
        descriptions.forEach((desc)->{
            IndexDesc res = new IndexDesc(desc.getFieldName(), desc.getIndexName(), desc.getIndexID());
            desc.getParamsList().forEach((kv)-> res.AddParam(kv.getKey(), kv.getValue()));
            results.add(res);
        });

        return results;
    }

    /**
     * Get index description by field name.
     * Return null if the field doesn't exist
     *
     * @return <code>IndexDesc</code> description of the index
     */
    public IndexDesc GetIndexDescByFieldName(@NonNull String name) {
        for (int i = 0; i < response.getIndexDescriptionsCount(); ++i) {
            IndexDescription desc = response.getIndexDescriptions(i);
            if (name.compareTo(desc.getFieldName()) == 0) {
                IndexDesc res = new IndexDesc(desc.getFieldName(), desc.getIndexName(), desc.getIndexID());
                desc.getParamsList().forEach((kv)-> res.AddParam(kv.getKey(), kv.getValue()));
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

        public IndexDesc(@NonNull String fieldName, String indexName, long id) {
            this.fieldName = fieldName;
            this.indexName = indexName;
            this.id = id;
        }

        public void AddParam(@NonNull String key, @NonNull String value) {
            this.params.put(key, value);
        }

        @Override
        public String toString() {
            return "(fieldName: " + fieldName + " indexName: " + indexName + " id: " + id + " params: " +
                    params.toString() + ")";
        }
    }

    /**
     * Construct a <code>String</code> by <code>DescIndexResponseWrapper</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Index description{" +
                GetIndexDescriptions().toString() +
                '}';
    }
}
