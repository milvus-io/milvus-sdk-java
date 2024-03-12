package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>createAlias</code> interface.
 */
@Getter
@ToString
public class CreateAliasParam {
    private final String collectionName;
    private final String databaseName;
    private final String alias;

    private CreateAliasParam(@NonNull CreateAliasParam.Builder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.alias = builder.alias;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateAliasParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String databaseName;
        private String alias;

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
         * Sets the collection alias. Collection alias cannot be empty or null.
         *
         * @param alias alias of the collection
         * @return <code>Builder</code>
         */
        public Builder withAlias(@NonNull String alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateAliasParam} instance.
         *
         * @return {@link CreateAliasParam}
         */
        public CreateAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new CreateAliasParam(this);
        }
    }

}
