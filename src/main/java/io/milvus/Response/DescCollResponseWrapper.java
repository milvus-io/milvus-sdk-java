package io.milvus.Response;

import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.collection.FieldType;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DescribeCollectionResponse;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>describeCollection</code> interface.
 */
public class DescCollResponseWrapper {
    private final DescribeCollectionResponse response;

    public DescCollResponseWrapper(@NonNull DescribeCollectionResponse response) {
        this.response = response;
    }

    /**
     * Get name of the collection.
     *
     * @return <code>String</code> name of the collection
     */
    public String GetCollectionName() {
        CollectionSchema schema = response.getSchema();
        return schema.getName();
    }

    /**
     * Get description of the collection.
     *
     * @return <code>String</code> description of the collection
     */
    public String GetCollectionDescription() {
        CollectionSchema schema = response.getSchema();
        return schema.getDescription();
    }

    /**
     * Get internal id of the collection.
     *
     * @return <code>long</code> internal id of the collection
     */
    public long GetCollectionID() {
        return response.getCollectionID();
    }

    /**
     * Get shard number of the collection.
     *
     * @return <code>int</code> shard number of the collection
     */
    public int GetShardNumber() {
        return response.getShardsNum();
    }

    /**
     * Get utc timestamp when collection created.
     *
     * @return <code>long</code> utc timestamp when collection created
     */
    public long GetCreatedUtcTimestamp() {
        return response.getCreatedUtcTimestamp();
    }

    /**
     * Get aliases of the collection.
     *
     * @return <code>List<String></String></code> aliases of the collection
     */
    public List<String> GetAliases() {
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < response.getAliasesCount(); ++i) {
            aliases.add(response.getAliases(i));
        }

        return aliases;
    }

    /**
     * Get schema of the collection's fields.
     *
     * @return <code>List<FieldType></code> schema of the collection's fields
     */
    public List<FieldType> GetFields() {
        List<FieldType> results = new ArrayList<>();
        CollectionSchema schema = response.getSchema();
        List<FieldSchema> fields = schema.getFieldsList();
        fields.forEach((field) -> results.add(convertField(field)));

        return results;
    }

    /**
     * Get schema of a field by name.
     * Return null if the field doesn't exist
     *
     * @return <code>FieldType</code> schema of the field
     */
    public FieldType GetField(@NonNull String name) {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (name.compareTo(field.getName()) == 0) {
                return convertField(field);
            }
        }

        return null;
    }

    /**
     * Convert a grpc field schema to client schema
     *
     * @return <code>FieldType</code> schema of the field
     */
    private FieldType convertField(@NonNull FieldSchema field) {
        FieldType.Builder builder = FieldType.newBuilder()
                .withName(field.getName())
                .withDescription(field.getDescription())
                .withPrimaryKey(field.getIsPrimaryKey())
                .withAutoID(field.getAutoID())
                .withDataType(field.getDataType());

        List<KeyValuePair> keyValuePairs = field.getTypeParamsList();
        keyValuePairs.forEach((kv) -> builder.addTypeParam(kv.getKey(), kv.getValue()));

        return builder.build();
    }

    /**
     * Construct a <code>String</code> by <code>DescCollResponseWrapper</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Collection Description{" +
                "name:'" + GetCollectionName() + '\'' +
                ", description:'" + GetCollectionDescription() + '\'' +
                ", id:" + GetCollectionID() +
                ", shardNumber:" + GetShardNumber() +
                ", createdUtcTimestamp:" + GetCreatedUtcTimestamp() +
                ", aliases:" + GetAliases() +
                ", fields:" + GetFields().toString() +
                '}';
    }
}
