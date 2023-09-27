package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
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
    private final String collectionName;
    private final Long flushTs;

    private GetFlushStateParam(@NonNull Builder builder) {
        this.segmentIDs = builder.segmentIDs;
        this.collectionName = builder.collectionName;
        this.flushTs = builder.flushTs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetFlushStateParam} class.
     */
    public static final class Builder {
        private final List<Long> segmentIDs = new ArrayList<>(); // deprecated
        private String collectionName;
        private Long flushTs = 0L;

        private Builder() {
        }

        /**
         * Specify segments
         *
         * @param segmentIDs segments id list
         * @return <code>Builder</code>
         */
        @Deprecated
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
        @Deprecated
        public Builder addSegmentID(@NonNull Long segmentID) {
            this.segmentIDs.add(segmentID);
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Input a time stamp of a flush action, get its flush state
         *
         * @param flushTs a time stamp returned by the flush() response
         * @return <code>Builder</code>
         */
        public Builder withFlushTs(@NonNull Long flushTs) {
            this.flushTs = flushTs;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetFlushStateParam} instance.
         *
         * @return {@link GetFlushStateParam}
         */
        public GetFlushStateParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetFlushStateParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link GetFlushStateParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetFlushStateParam{" +
                "collectionName='" + collectionName + '\'' +
                ", flushTs=" + flushTs +
                '}';
    }
}
