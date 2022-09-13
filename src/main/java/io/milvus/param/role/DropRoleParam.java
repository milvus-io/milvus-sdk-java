package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class DropRoleParam {

    private final String roleName;

    private DropRoleParam(@NonNull DropRoleParam.Builder builder) {
        this.roleName = builder.roleName;
    }

    public static DropRoleParam.Builder newBuilder() {
        return new DropRoleParam.Builder();
    }

    /**
     * Builder for {@link DropRoleParam} class.
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
        public DropRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DropRoleParam} instance.
         *
         * @return {@link DropRoleParam}
         */
        public DropRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new DropRoleParam(this);
        }
    }

}
