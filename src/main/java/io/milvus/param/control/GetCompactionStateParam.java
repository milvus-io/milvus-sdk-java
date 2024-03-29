package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;


/**
 * Parameters for <code>getCompactionState</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Compaction function design</a>
 */
@Getter
@ToString
public class GetCompactionStateParam {
    private final Long compactionID;

    private GetCompactionStateParam(@NonNull Builder builder) {
        this.compactionID = builder.compactionID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetCompactionStateParam} class.
     */
    public static final class Builder {
        private Long compactionID;

        private Builder() {
        }

        /**
         * Sets the compaction action id to get state.
         *
         * @param compactionID compaction action id
         * @return <code>Builder</code>
         */
        public Builder withCompactionID(@NonNull Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetCompactionStateParam} instance.
         *
         * @return {@link GetCompactionStateParam}
         */
        public GetCompactionStateParam build() throws ParamException {
            return new GetCompactionStateParam(this);
        }
    }
}
