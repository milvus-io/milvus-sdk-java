package io.milvus.param.credential;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class UpdateCredentialParam {
    private final String username;
    private final String oldPassword;
    private final String newPassword;

    private UpdateCredentialParam(@NonNull UpdateCredentialParam.Builder builder) {
        this.username = builder.username;
        this.oldPassword = builder.oldPassword;
        this.newPassword = builder.newPassword;
    }

    public static UpdateCredentialParam.Builder newBuilder() {
        return new UpdateCredentialParam.Builder();
    }

    /**
     * Builder for {@link UpdateCredentialParam} class.
     */
    public static final class Builder {
        private String username;
        private String oldPassword;
        private String newPassword;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withUsername(@NonNull String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the old password. Old password cannot be empty or null.
         *
         * @param password old password
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withOldPassword(@NonNull String password) {
            this.oldPassword = password;
            return this;
        }

        /**
         * Sets the new password. New password cannot be empty or null.
         *
         * @param password password
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withNewPassword(@NonNull String password) {
            this.newPassword = password;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link UpdateCredentialParam} instance.
         *
         * @return {@link UpdateCredentialParam}
         */
        public UpdateCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");
            ParamUtils.CheckNullEmptyString(oldPassword, "OldPassword");
            ParamUtils.CheckNullEmptyString(newPassword, "NewPassword");

            return new UpdateCredentialParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link UpdateCredentialParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "UpdateCredentialParam{" +
                "username='" + username + '\'' +
                ", oldPassword='" + oldPassword + '\'' +
                ", newPassword='" + newPassword + '\'' +
                '}';
    }
}
