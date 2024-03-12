package io.milvus.param.credential;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class DeleteCredentialParam {
    private final String username;

    private DeleteCredentialParam(@NonNull DeleteCredentialParam.Builder builder) {
        this.username = builder.username;
    }

    public static DeleteCredentialParam.Builder newBuilder() {
        return new DeleteCredentialParam.Builder();
    }

    /**
     * Builder for {@link DeleteCredentialParam} class.
     */
    public static final class Builder {
        private String username;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public DeleteCredentialParam.Builder withUsername(@NonNull String username) {
            this.username = username;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DeleteCredentialParam} instance.
         *
         * @return {@link DeleteCredentialParam}
         */
        public DeleteCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");

            return new DeleteCredentialParam(this);
        }
    }

}
