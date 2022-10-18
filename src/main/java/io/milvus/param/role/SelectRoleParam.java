package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class SelectRoleParam {

    private final String roleName;
    private final boolean includeUserInfo;

    private SelectRoleParam(@NonNull SelectRoleParam.Builder builder) {
        this.roleName = builder.roleName;
        this.includeUserInfo = builder.includeUserInfo;
    }

    public static SelectRoleParam.Builder newBuilder() {
        return new SelectRoleParam.Builder();
    }

    /**
     * Builder for {@link SelectRoleParam} class.
     */
    public static final class Builder {
        private String roleName;
        private boolean includeUserInfo;

        private Builder() {
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public SelectRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the includeUserInfo. includeUserInfo default false.
         *
         * @param includeUserInfo includeUserInfo
         * @return <code>Builder</code>
         */
        public SelectRoleParam.Builder withIncludeUserInfo(boolean includeUserInfo) {
            this.includeUserInfo = includeUserInfo;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectRoleParam} instance.
         *
         * @return {@link SelectRoleParam}
         */
        public SelectRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new SelectRoleParam(this);
        }
    }

}