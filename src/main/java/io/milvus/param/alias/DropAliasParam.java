package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>dropAlias</code> interface.
 */
@Getter
@ToString
public class DropAliasParam {
    private final String alias;
    private final String databaseName;

    private DropAliasParam(@NonNull Builder builder) {
        this.alias = builder.alias;
        this.databaseName = builder.databaseName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link DropAliasParam} class.
     */
    public static final class Builder {
        private String alias;
        private String databaseName;

        private Builder() {
        }

        /**
         * Sets collection alias. Collection alias cannot be empty or null.
         *
         * @param alias alias of the collection
         * @return <code>Builder</code>
         */
        public Builder withAlias(@NonNull String alias) {
            this.alias = alias;
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
         * Verifies parameters and creates a new {@link DropAliasParam} instance.
         *
         * @return {@link DropAliasParam}
         */
        public DropAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new DropAliasParam(this);
        }
    }

}
