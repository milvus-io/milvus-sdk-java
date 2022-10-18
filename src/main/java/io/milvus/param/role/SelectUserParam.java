package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class SelectUserParam {

    private final String userName;

    private final boolean includeRoleInfo;

    private SelectUserParam(@NonNull SelectUserParam.Builder builder) {
        this.userName = builder.userName;
        this.includeRoleInfo = builder.includeRoleInfo;
    }

    public static SelectUserParam.Builder newBuilder() {
        return new SelectUserParam.Builder();
    }

    /**
     * Builder for {@link SelectUserParam} class.
     */
    public static final class Builder {
        private String userName;
        private boolean includeRoleInfo;

        private Builder() {
        }

        /**
         * Sets the userName. userName cannot be empty or null.
         *
         * @param userName userName
         * @return <code>Builder</code>
         */
        public SelectUserParam.Builder withUserName(@NonNull String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the includeRoleInfo. includeRoleInfo default false.
         *
         * @param includeRoleInfo includeRoleInfo
         * @return <code>Builder</code>
         */
        public SelectUserParam.Builder withIncludeRoleInfo(boolean includeRoleInfo) {
            this.includeRoleInfo = includeRoleInfo;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectUserParam} instance.
         *
         * @return {@link SelectUserParam}
         */
        public SelectUserParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(userName, "UserName");

            return new SelectUserParam(this);
        }
    }

}