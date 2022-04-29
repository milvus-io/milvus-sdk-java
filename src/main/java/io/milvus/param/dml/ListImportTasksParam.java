package io.milvus.param.dml;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getImportState</code> interface.
 */
@Getter
public class ListImportTasksParam {

    private ListImportTasksParam(@NonNull Builder builder) {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ListImportTasksParam} class.
     */
    public static class Builder {
        private Builder() {
        }

        public ListImportTasksParam build() throws ParamException {
            return new ListImportTasksParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link ListImportTasksParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ListImportTasksParam";
    }
}
