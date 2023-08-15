package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>getFlushAllState</code> interface.
 */
@Getter
public class GetFlushAllStateParam {
    private final long flushAllTs;

    private GetFlushAllStateParam(@NonNull Builder builder) {
        this.flushAllTs = builder.flushAllTs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetFlushAllStateParam} class.
     */
    public static final class Builder {
        private Long flushAllTs;

        private Builder() {
        }

        public Builder withFlushAllTs(@NonNull Long flushAllTs) {
            this.flushAllTs = flushAllTs;
            return this;
        }


        /**
         * Verifies parameters and creates a new {@link GetFlushAllStateParam} instance.
         *
         * @return {@link GetFlushAllStateParam}
         */
        public GetFlushAllStateParam build() throws ParamException {
            return new GetFlushAllStateParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link GetFlushAllStateParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetFlushAllStateParam{" +
                "flushAllTs=" + flushAllTs +
                '}';
    }
}