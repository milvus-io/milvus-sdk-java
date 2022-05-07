package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getReplicas</code> interface.
 */
@Getter
public class GetReplicasParam {
    private final String collectionName;
    private boolean withShardNodes;

    private GetReplicasParam(@NonNull Builder builder) {
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
        private String collectionName;

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
         * Verifies parameters and creates a new {@link GetReplicasParam} instance.
         *
         * @return {@link GetReplicasParam}
         */
        public GetReplicasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetReplicasParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link GetReplicasParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetReplicasParam{" +
                "collectionName='" + collectionName + '\'' +
                '}';
    }
}
