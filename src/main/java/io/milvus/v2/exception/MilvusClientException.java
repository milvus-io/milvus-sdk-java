package io.milvus.v2.exception;

import lombok.Getter;

@Getter
public class MilvusClientException extends RuntimeException {

    private final ErrorCode errorCode;

    public MilvusClientException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }


}
