package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class GrantRolePrivilegeParam {

    private final String roleName;

    private final String object;

    private final String objectName;

    private final String privilege;

    private GrantRolePrivilegeParam(@NonNull GrantRolePrivilegeParam.Builder builder) {
        this.roleName = builder.roleName;
        this.object = builder.object;
        this.objectName = builder.objectName;
        this.privilege = builder.privilege;
    }

    public static GrantRolePrivilegeParam.Builder newBuilder() {
        return new GrantRolePrivilegeParam.Builder();
    }

    /**
     * Builder for {@link GrantRolePrivilegeParam} class.
     */
    public static final class Builder {
        private String roleName;
        private String object;
        private String objectName;
        private String privilege;


        private Builder() {
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public GrantRolePrivilegeParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the object. object cannot be empty or null.
         *
         * @param object object
         * @return <code>Builder</code>
         */
        public GrantRolePrivilegeParam.Builder withObject(@NonNull String object) {
            this.object = object;
            return this;
        }

        /**
         * Sets the objectName. objectName cannot be empty or null.
         *
         * @param objectName objectName
         * @return <code>Builder</code>
         */
        public GrantRolePrivilegeParam.Builder withObjectName(@NonNull String objectName) {
            this.objectName = objectName;
            return this;
        }

        /**
         * Sets the privilege. privilege cannot be empty or null.
         *
         * @param privilege privilege
         * @return <code>Builder</code>
         */
        public GrantRolePrivilegeParam.Builder withPrivilege(@NonNull String privilege) {
            this.privilege = privilege;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GrantRolePrivilegeParam} instance.
         *
         * @return {@link GrantRolePrivilegeParam}
         */
        public GrantRolePrivilegeParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");
            ParamUtils.CheckNullEmptyString(object, "Object");
            ParamUtils.CheckNullEmptyString(objectName, "ObjectName");
            ParamUtils.CheckNullEmptyString(privilege, "Privilege");

            return new GrantRolePrivilegeParam(this);
        }
    }

}
