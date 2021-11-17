package io.milvus.Response;

import io.milvus.exception.ParamException;
import io.milvus.grpc.*;

import lombok.NonNull;

import java.util.List;

/**
 * Util class to wrap response of <code>query</code> interface.
 */
public class QueryResultsWrapper {
    private final QueryResults results;

    public QueryResultsWrapper(@NonNull QueryResults results) {
        this.results = results;
    }

    /**
     * Get {@link FieldDataWrapper} for a field.
     * Throws {@link ParamException} if the field doesn't exist.
     *
     * @return <code>FieldDataWrapper</code>
     */
    public FieldDataWrapper getFieldWrapper(@NonNull String fieldName) throws ParamException {
        List<FieldData> fields = results.getFieldsDataList();
        for (FieldData field : fields) {
            if (fieldName.compareTo(field.getFieldName()) == 0) {
                return new FieldDataWrapper(field);
            }
        }

        throw new ParamException("The field name doesn't exist");
    }
}
