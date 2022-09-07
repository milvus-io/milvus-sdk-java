package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class GetRoleParam {

    private final String roleName;

    private GetRoleParam(@NonNull GetRoleParam.Builder builder) {
        this.roleName = builder.roleName;
    }

    public static GetRoleParam.Builder newBuilder() {
        return new GetRoleParam.Builder();
    }

    /**
     * Builder for {@link GetRoleParam} class.
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
        public GetRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetRoleParam} instance.
         *
         * @return {@link GetRoleParam}
         */
        public GetRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new GetRoleParam(this);
        }
    }

}
