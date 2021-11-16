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
     * Builder for <code>DropAliasParam</code> class.
     */
    public static final class Builder {
        private String alias;

        private Builder() {
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
         * Verify parameters and create a new <code>DropAliasParam</code> instance.
         *
         * @return <code>DropAliasParam</code>
         */
        public DropAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new DropAliasParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>DropAliasParam</code> instance.
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
