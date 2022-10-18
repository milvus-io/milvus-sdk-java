package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class RemoveUserFromRoleParam {
    private final String userName;

    private final String roleName;

    private RemoveUserFromRoleParam(@NonNull RemoveUserFromRoleParam.Builder builder) {
        this.userName = builder.userName;
        this.roleName = builder.roleName;
    }

    public static RemoveUserFromRoleParam.Builder newBuilder() {
        return new RemoveUserFromRoleParam.Builder();
    }

    /**
     * Builder for {@link RemoveUserFromRoleParam} class.
     */
    public static final class Builder {
        private String userName;
        private String roleName;

        private Builder() {
        }

        /**
         * Sets the username. UserName cannot be empty or null.
         *
         * @param userName userName
         * @return <code>Builder</code>
         */
        public RemoveUserFromRoleParam.Builder withUserName(@NonNull String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public RemoveUserFromRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link RemoveUserFromRoleParam} instance.
         *
         * @return {@link RemoveUserFromRoleParam}
         */
        public RemoveUserFromRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(userName, "UserName");
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new RemoveUserFromRoleParam(this);
        }
    }

}