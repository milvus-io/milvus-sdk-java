package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

@Getter
public class FlushParam {
    private final List<String> collectionNames;
    private final Boolean syncFlush;
    private final long syncFlushWaitingInterval;
    private final long syncFlushWaitingTimeout;

    private FlushParam(@NonNull Builder builder) {
        this.collectionNames = builder.collectionNames;
        this.syncFlush = builder.syncFlush;
        this.syncFlushWaitingInterval = builder.syncFlushWaitingInterval.longValue();
        this.syncFlushWaitingTimeout = builder.syncFlushWaitingTimeout.longValue();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> collectionNames = new ArrayList<String>();

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

        public Builder withCollectionNames(@NonNull List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public Builder addCollectionName(@NonNull String collectionName) {
            this.collectionNames.add(collectionName);
            return this;
        }

        public Builder withSyncFlush(@NonNull Boolean syncFlush) {
            this.syncFlush = syncFlush;
            return this;
        }

        public Builder withSyncFlushWaitingInterval(@NonNull Long milliseconds) {
            this.syncFlushWaitingInterval = milliseconds;
            return this;
        }

        public Builder withSyncFlushWaitingTimeout(@NonNull Long seconds) {
            this.syncFlushWaitingTimeout = seconds;
            return this;
        }

        public FlushParam build() throws ParamException {
            if (collectionNames == null || collectionNames.isEmpty()) {
                throw new ParamException("CollectionNames can not be empty");
            }

            for (String name : collectionNames) {
                ParamUtils.CheckNullEmptyString(name, "Collection name");
            }

            if (syncFlush == Boolean.TRUE) {
                if (syncFlushWaitingInterval <= 0) {
                    throw new ParamException("Sync flush waiting interval must be larger than zero");
                } else if (syncFlushWaitingInterval > Constant.MAX_WAITING_FLUSHING_INTERVAL) {
                    throw new ParamException("Sync flush waiting interval must be small than "
                            + Constant.MAX_WAITING_FLUSHING_INTERVAL.toString() + " milliseconds");
                }

                if (syncFlushWaitingTimeout <= 0) {
                    throw new ParamException("Sync flush waiting timeout must be larger than zero");
                } else if (syncFlushWaitingTimeout > Constant.MAX_WAITING_FLUSHING_TIMEOUT) {
                    throw new ParamException("Sync flush waiting timeout must be small than "
                            + Constant.MAX_WAITING_FLUSHING_TIMEOUT.toString() + " seconds");
                }
            }

            return new FlushParam(this);
        }
    }

    @Override
    public String toString() {
        return "FlushParam{" +
                "collectionNames='" + collectionNames + '\'' +
                ", syncFlush=" + syncFlush.toString() +
                ", syncFlushWaitingInterval=" + syncFlushWaitingInterval +
                '}';
    }
}
