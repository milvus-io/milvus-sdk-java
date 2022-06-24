package io.milvus.param.credential;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class CreateCredentialParam {
    private final String username;

    private final String password;

    private CreateCredentialParam(@NonNull CreateCredentialParam.Builder builder) {
        this.username = builder.username;
        this.password = builder.password;
    }

    public static CreateCredentialParam.Builder newBuilder() {
        return new CreateCredentialParam.Builder();
    }

    /**
     * Builder for {@link CreateCredentialParam} class.
     */
    public static final class Builder {
        private String username;
        private String password;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public CreateCredentialParam.Builder withUsername(@NonNull String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password. Password cannot be empty or null.
         *
         * @param password password
         * @return <code>Builder</code>
         */
        public CreateCredentialParam.Builder withPassword(@NonNull String password) {
            this.password = password;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateCredentialParam} instance.
         *
         * @return {@link CreateCredentialParam}
         */
        public CreateCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");
            ParamUtils.CheckNullEmptyString(password, "Password");

            return new CreateCredentialParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link CreateCredentialParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "CreateCredentialParam{" +
                "username='" + username +
                '}';
    }
}
