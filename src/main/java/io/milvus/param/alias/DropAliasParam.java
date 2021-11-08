package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class DropAliasParam {
    private final String alias;

    private DropAliasParam(@NonNull Builder builder) {
        this.alias = builder.alias;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String alias;

        private Builder() {
        }

        public Builder withAlias(@NonNull String alias) {
            this.alias = alias;
            return this;
        }

        public DropAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new DropAliasParam(this);
        }
    }

    @Override
    public String toString() {
        return "DropAliasParam{" +
                ", alias='" + alias + '\'' +
                '}';
    }
}
