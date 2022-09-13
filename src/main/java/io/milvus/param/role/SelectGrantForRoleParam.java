package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class SelectGrantForRoleParam {

    private final String roleName;

    private SelectGrantForRoleParam(@NonNull SelectGrantForRoleParam.Builder builder) {
        this.roleName = builder.roleName;
    }

    public static SelectGrantForRoleParam.Builder newBuilder() {
        return new SelectGrantForRoleParam.Builder();
    }

    /**
     * Builder for {@link SelectGrantForRoleParam} class.
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
        public SelectGrantForRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectGrantForRoleParam} instance.
         *
         * @return {@link SelectGrantForRoleParam}
         */
        public SelectGrantForRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new SelectGrantForRoleParam(this);
        }
    }

}
