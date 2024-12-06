package io.milvus.v2.service.vector.request.data;

import io.milvus.grpc.PlaceholderType;

public class EmbeddedText implements BaseVector {
    private final String data;

    public EmbeddedText(String data) {
        this.data = data;
    }
    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.VarChar;
    }

    @Override
    public Object getData() {
        return this.data;
    }
}
