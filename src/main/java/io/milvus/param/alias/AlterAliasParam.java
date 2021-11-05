package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;

public class AlterAliasParam {
    private final String collectionName;
    private final String alias;

    private AlterAliasParam(@Nonnull AlterAliasParam.Builder builder) {
        this.collectionName = builder.collectionName;
        this.alias = builder.alias;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getAlias() {
        return alias;
    }

    public static final class Builder {
        private String collectionName;
        private String alias;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withAlias(@Nonnull String alias) {
            this.alias = alias;
            return this;
        }

        public AlterAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new AlterAliasParam(this);
        }
    }
}
