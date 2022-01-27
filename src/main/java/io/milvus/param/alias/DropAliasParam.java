package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>dropAlias</code> interface.
 */
@Getter
public class DropAliasParam {
    private final String alias;

    private DropAliasParam(@NonNull Builder builder) {
        this.alias = builder.alias;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link DropAliasParam} class.
     */
    public static final class Builder {
        private String alias;

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
         * Verifies parameters and creates a new {@link DropAliasParam} instance.
         *
         * @return {@link DropAliasParam}
         */
        public DropAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new DropAliasParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link DropAliasParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "DropAliasParam{" +
                ", alias='" + alias + '\'' +
                '}';
    }
}
