package io.milvus.param.dml;

import com.alibaba.fastjson.JSONObject;
import io.milvus.exception.ParamException;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;


/**
 * Parameters for <code>upsert</code> interface.
 */
public class UpsertParam extends InsertParam {
    private UpsertParam(@NonNull Builder builder) {
        super(builder);
    }

    public static UpsertParam.Builder newBuilder() {
        return new UpsertParam.Builder();
    }

    /**
     * Builder for {@link UpsertParam} class.
     */
    public static class Builder extends InsertParam.Builder {
        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            super.withDatabaseName(databaseName);
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            super.withCollectionName(collectionName);
            return this;
        }

        /**
         * Set partition name (Optional).
         * This partition name will be ignored if the collection has a partition key field.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            super.withPartitionName(partitionName);
            return this;
        }

        /**
         * Sets the column data to insert. The field list cannot be empty.
         *
         * @param fields insert column data
         * @return <code>Builder</code>
         * @see InsertParam.Field
         */
        public Builder withFields(@NonNull List<Field> fields) {
            super.withFields(fields);
            return this;
        }

        /**
         * Sets the row data to insert. The rows list cannot be empty.
         *
         * @param rows insert row data
         * @return <code>Builder</code>
         * @see JSONObject
         */
        public Builder withRows(@NonNull List<JSONObject> rows) {
            super.withRows(rows);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link UpsertParam} instance.
         *
         * @return {@link UpsertParam}
         */
        public UpsertParam build() throws ParamException {
            super.build();
            return new UpsertParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link UpsertParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        String baseStr = "UpsertParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", rowCount=" + rowCount;
        if (!CollectionUtils.isEmpty(fields)) {
            return baseStr +
                    ", columnFields+" + fields +
                    '}';
        } else {
            return baseStr + '}';
        }
    }
}
