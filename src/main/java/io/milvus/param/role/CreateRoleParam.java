package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class CreateRoleParam {

    private final String roleName;

    private CreateRoleParam(@NonNull CreateRoleParam.Builder builder) {
        this.roleName = builder.roleName;
    }

    public static CreateRoleParam.Builder newBuilder() {
        return new CreateRoleParam.Builder();
    }

    /**
     * Builder for {@link CreateRoleParam} class.
     */
    public static final class Builder {
        private String roleName;

        private Builder() {
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public CreateRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateRoleParam} instance.
         *
         * @return {@link CreateRoleParam}
         */
        public CreateRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new CreateRoleParam(this);
        }
    }

}
