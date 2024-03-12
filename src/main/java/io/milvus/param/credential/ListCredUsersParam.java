package io.milvus.param.credential;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class ListCredUsersParam {

    private ListCredUsersParam(@NonNull ListCredUsersParam.Builder builder) {
    }

    public static ListCredUsersParam.Builder newBuilder() {
        return new ListCredUsersParam.Builder();
    }

    /**
     * Builder for {@link ListCredUsersParam} class.
     */
    public static final class Builder {

        private Builder() {
        }

        /**
         * Verifies parameters and creates a new {@link ListCredUsersParam} instance.
         *
         * @return {@link ListCredUsersParam}
         */
        public ListCredUsersParam build() throws ParamException {
            return new ListCredUsersParam(this);
        }
    }

}
