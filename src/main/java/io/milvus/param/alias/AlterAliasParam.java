package io.milvus.param.alias;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

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

    public static final class Builder {
        private String collectionName;
        private String alias;

        private Builder() {
        }

        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withAlias(@NonNull String alias) {
            this.alias = alias;
            return this;
        }

        public AlterAliasParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(alias, "Alias");

            return new AlterAliasParam(this);
        }
    }

    @Override
    public String toString() {
        return "AlterAliasParam{" +
                "collectionName='" + collectionName + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
