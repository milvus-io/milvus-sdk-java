package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;

public class DropAliasParam {
    private final String alias;

    private DropAliasParam(@Nonnull Builder builder) {
        this.alias = builder.alias;
    }

    public String getAlias() {
        return alias;
    }

    public static final class Builder {
        private String alias;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withAlias(@Nonnull String alias) {
            this.alias = alias;
            return this;
        }

        public DropAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new DropAliasParam(this);
        }
    }
}
