package io.milvus.Response;

import io.milvus.grpc.FieldData;
import lombok.NonNull;

public class FieldDataWrapper {
    private final FieldData fieldData;

    public FieldDataWrapper(@NonNull FieldData fieldData) {
        this.fieldData = fieldData;
    }
}
