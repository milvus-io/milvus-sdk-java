package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;


/**
 * Parameters for <code>getCompactionState</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Metric function design</a>
 */
@Getter
public class GetCompactionStateParam {
    private final Long compactionID;

    private GetCompactionStateParam(@NonNull Builder builder) {
        this.compactionID = builder.compactionID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Construct a <code>String</code> by <code>GetCompactionStateParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetCompactionStateParam{" +
                "compactionID='" + compactionID + '\'' +
                '}';
    }

    /**
     * Builder for <code>GetCompactionStateParam</code> class.
     */
    public static final class Builder {
        private Long compactionID;

        private Builder() {
        }

        /**
         * Set compaction action id to get state.
         *
         * @param compactionID compaction action id
         * @return <code>Builder</code>
         */
        public Builder withCompactionID(@NonNull Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        /**
         * Verify parameters and create a new <code>GetCompactionStateParam</code> instance.
         *
         * @return <code>GetCompactionStateParam</code>
         */
        public GetCompactionStateParam build() throws ParamException {
            return new GetCompactionStateParam(this);
        }
    }
}
