package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.collection.ShowCollectionsParam;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getCompactionStateWithPlans</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Compaction function design</a>
 */
@Getter
public class GetCompactionPlansParam {
    private final Long compactionID;

    private GetCompactionPlansParam(@NonNull Builder builder) {
        this.compactionID = builder.compactionID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Constructs a <code>String</code> by {@link GetCompactionPlansParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetCompactionPlansParam{" +
                "compactionID='" + compactionID + '\'' +
                '}';
    }

    /**
     * Builder for {@link GetCompactionPlansParam} class.
     */
    public static final class Builder {
        private Long compactionID;

        private Builder() {
        }

        /**
         * Sets compaction action id to get the plans.
         *
         * @param compactionID compaction action id
         * @return <code>Builder</code>
         */
        public Builder withCompactionID(@NonNull Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetCompactionPlansParam} instance.
         *
         * @return {@link GetCompactionPlansParam}
         */
        public GetCompactionPlansParam build() throws ParamException {
            return new GetCompactionPlansParam(this);
        }
    }
}
