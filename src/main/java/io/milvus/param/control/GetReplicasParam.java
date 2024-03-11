package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>getReplicas</code> interface.
 */
@Getter
@ToString
public class GetReplicasParam {
    private final String databaseName;
    private final String collectionName;
    private boolean withShardNodes;

    private GetReplicasParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.withShardNodes = true;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetReplicasParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;

        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
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
         * Verifies parameters and creates a new {@link GetReplicasParam} instance.
         *
         * @return {@link GetReplicasParam}
         */
        public GetReplicasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetReplicasParam(this);
        }
    }

}
