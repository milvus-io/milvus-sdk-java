package io.milvus.param.dml;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getBulkloadState</code> interface.
 */
@Getter
public class ListBulkloadTasksParam {

    private ListBulkloadTasksParam(@NonNull Builder builder) {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ListBulkloadTasksParam} class.
     */
    public static class Builder {
        private Builder() {
        }

        public ListBulkloadTasksParam build() throws ParamException {
            return new ListBulkloadTasksParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link ListBulkloadTasksParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ListBulkloadTasksParam";
    }
}
