package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>alterAlias</code> interface.
 */
@Getter
public class AlterAliasParam {
    private final String collectionName;
    private final String alias;

    private AlterAliasParam(@NonNull AlterAliasParam.Builder builder) {
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>AlterAliasParam</code> class.
     */
    public static final class Builder {
        private String collectionName;
        private String alias;

        private Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Set alias, alias cannot be empty or null.
         *
         * @param alias alias of the collection
         * @return <code>Builder</code>
         */
        public Builder withAlias(@NonNull String alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Verify parameters and create a new <code>AlterAliasParam</code> instance.
         *
         * @return <code>AlterAliasParam</code>
         */
        public AlterAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new AlterAliasParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>AlterAliasParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "AlterAliasParam{" +
                "collectionName='" + collectionName + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
