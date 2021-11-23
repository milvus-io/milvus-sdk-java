package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>flush</code> interface.
 * Note that the flush interface is not exposed currently.
 */
@Getter
public class FlushParam {
    private final List<String> collectionNames;
    private final Boolean syncFlush;
    private final long syncFlushWaitingInterval;
    private final long syncFlushWaitingTimeout;

    private FlushParam(@NonNull Builder builder) {
        this.collectionNames = builder.collectionNames;
        this.syncFlush = builder.syncFlush;
        this.syncFlushWaitingInterval = builder.syncFlushWaitingInterval;
        this.syncFlushWaitingTimeout = builder.syncFlushWaitingTimeout;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>FlushParam</code> class.
     */
    public static final class Builder {
        private final List<String> collectionNames = new ArrayList<>();

        // syncFlush:
        //   Default behavior is sync flushing, flush() return after collection finish flushing.
        private Boolean syncFlush = Boolean.TRUE;

        // syncFlushWaitingInterval:
        //   When syncFlush is ture, flush() will wait until collection finish flushing,
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncFlushWaitingInterval = 500L;

        // syncFlushWaitingTimeout:
        //   When syncFlush is ture, flush() will wait until collection finish flushing,
        //   this value control the waiting timeout. Unit: second. Default value: 60 seconds.
        private Long syncFlushWaitingTimeout = 60L;

        private Builder() {
        }

        /**
         * Set a list of collections to be flushed.
         *
         * @param collectionNames a list of collections
         * @return <code>Builder</code>
         */
        public Builder withCollectionNames(@NonNull List<String> collectionNames) {
            this.collectionNames.addAll(collectionNames);
            return this;
        }

        /**
         * Add a collections to be flushed.
         *
         * @param collectionName name of the collections
         * @return <code>Builder</code>
         */
        public Builder addCollectionName(@NonNull String collectionName) {
            this.collectionNames.add(collectionName);
            return this;
        }

        /**
         * Set flush action to sync mode.
         * With sync mode, the client side will keep waiting until all segments of the collection successfully flushed.
         *
         * If not sync mode, client will return at once after the flush() is called.
         *
         * @param syncFlush <code>Boolean.TRUE</code> is sync mode, Bollean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withSyncFlush(@NonNull Boolean syncFlush) {
            this.syncFlush = syncFlush;
            return this;
        }

        /**
         * Set waiting interval in sync mode. In sync mode, the client will constantly check segments state by interval.
         * Interval must be larger than zero, and cannot be larger than Constant.MAX_WAITING_FLUSHING_INTERVAL.
         * @see Constant
         *
         * @param milliseconds interval
         * @return <code>Builder</code>
         */
        public Builder withSyncFlushWaitingInterval(@NonNull Long milliseconds) {
            this.syncFlushWaitingInterval = milliseconds;
            return this;
        }

        /**
         * Set time out value for sync mode.
         * Time out value must be larger than zero, and cannot be larger than Constant.MAX_WAITING_FLUSHING_TIMEOUT.
         * @see Constant
         *
         * @param seconds time out value for sync mode
         * @return <code>Builder</code>
         */
        public Builder withSyncFlushWaitingTimeout(@NonNull Long seconds) {
            this.syncFlushWaitingTimeout = seconds;
            return this;
        }

        /**
         * Verify parameters and create a new <code>FlushParam</code> instance.
         *
         * @return <code>FlushParam</code>
         */
        public FlushParam build() throws ParamException {
            if (collectionNames.isEmpty()) {
                throw new ParamException("CollectionNames can not be empty");
            }

            for (String name : collectionNames) {
                ParamUtils.CheckNullEmptyString(name, "Collection name");
            }

            if (syncFlush == Boolean.TRUE) {
                if (syncFlushWaitingInterval <= 0) {
                    throw new ParamException("Sync flush waiting interval must be larger than zero");
                } else if (syncFlushWaitingInterval > Constant.MAX_WAITING_FLUSHING_INTERVAL) {
                    throw new ParamException("Sync flush waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_FLUSHING_INTERVAL.toString() + " milliseconds");
                }

                if (syncFlushWaitingTimeout <= 0) {
                    throw new ParamException("Sync flush waiting timeout must be larger than zero");
                } else if (syncFlushWaitingTimeout > Constant.MAX_WAITING_FLUSHING_TIMEOUT) {
                    throw new ParamException("Sync flush waiting timeout cannot be larger than "
                            + Constant.MAX_WAITING_FLUSHING_TIMEOUT.toString() + " seconds");
                }
            }

            return new FlushParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>FlushParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "FlushParam{" +
                "collectionNames='" + collectionNames + '\'' +
                ", syncFlush=" + syncFlush.toString() +
                ", syncFlushWaitingInterval=" + syncFlushWaitingInterval +
                '}';
    }
}
