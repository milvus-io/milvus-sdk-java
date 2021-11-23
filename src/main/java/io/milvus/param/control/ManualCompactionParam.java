package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>manualCompaction</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Metric function design</a>
 */
@Getter
public class ManualCompactionParam {
    private final Long collectionID;

    private ManualCompactionParam(@NonNull Builder builder) {
        this.collectionID = builder.collectionID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Construct a <code>String</code> by <code>ManualCompactionParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ManualCompactionParam{" +
                "collectionID='" + collectionID + '\'' +
                '}';
    }

    /**
     * Builder for <code>ManualCompactionParam</code> class.
     */
    public static final class Builder {
        private Long collectionID;

        private Builder() {
        }

        /**
         * Ask server to compact a collection.
         *
         * @param collectionID target collection id
         * @return <code>Builder</code>
         */
        public Builder withCollectionID(@NonNull Long collectionID) {
            this.collectionID = collectionID;
            return this;
        }

        /**
         * Verify parameters and create a new <code>ManualCompactionParam</code> instance.
         *
         * @return <code>ManualCompactionParam</code>
         */
        public ManualCompactionParam build() throws ParamException {
            return new ManualCompactionParam(this);
        }
    }
}
