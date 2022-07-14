package io.milvus.param.control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>manualCompact</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Compaction function design</a>
 */
@Getter
public class ManualCompactParam {
    private final String collectionName;

    private ManualCompactParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Constructs a <code>String</code> by {@link ManualCompactParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ManualCompactionParam{" +
                "collectionName='" + collectionName + '\'' +
                '}';
    }

    /**
     * Builder for {@link ManualCompactParam} class.
     */
    public static final class Builder {
        private String collectionName;

        private Builder() {
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
         * Verifies parameters and creates a new {@link ManualCompactParam} instance.
         *
         * @return {@link ManualCompactParam}
         */
        public ManualCompactParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new ManualCompactParam(this);
        }
    }
}
