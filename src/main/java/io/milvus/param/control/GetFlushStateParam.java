package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>getMetric</code> interface.
 */
@Getter
public class GetFlushStateParam {
    private final List<Long> segmentIDs;

    private GetFlushStateParam(@NonNull Builder builder) {
        this.segmentIDs = builder.segmentIDs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>GetFlushStateParam</code> class.
     */
    public static final class Builder {
        private final List<Long> segmentIDs = new ArrayList<>();

        private Builder() {
        }

        /**
         * Specify segments
         *
         * @param segmentIDs segments id list
         * @return <code>Builder</code>
         */
        public Builder withSegmentIDs(@NonNull List<Long> segmentIDs) {
            this.segmentIDs.addAll(segmentIDs);
            return this;
        }

        /**
         * Specify a segment
         *
         * @param segmentID segment id
         * @return <code>Builder</code>
         */
        public Builder addSegmentID(@NonNull Long segmentID) {
            this.segmentIDs.add(segmentID);
            return this;
        }

        /**
         * Verifies parameters and creates a new <code>GetFlushStateParam</code> instance.
         *
         * @return <code>GetFlushStateParam</code>
         */
        public GetFlushStateParam build() throws ParamException {
            if (segmentIDs.isEmpty()) {
                throw new ParamException("Segment id array cannot be empty");
            }

            return new GetFlushStateParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by <code>GetFlushStateParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetFlushStateParam{" +
                "segmentIDs=" + segmentIDs.toString() +
                '}';
    }
}
