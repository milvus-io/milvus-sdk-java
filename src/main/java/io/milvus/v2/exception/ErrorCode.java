package io.milvus.v2.exception;

import lombok.Getter;

@Getter
public enum ErrorCode {
    SUCCESS(0),
    COLLECTION_NOT_FOUND(1),
    SERVER_ERROR(2);

    private final int code;

    ErrorCode(int i) {
        this.code = i;
    }
}
