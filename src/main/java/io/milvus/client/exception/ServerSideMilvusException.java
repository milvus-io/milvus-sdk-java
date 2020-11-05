package io.milvus.client.exception;

import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.Status;

/** Milvus exception from server side */
public class ServerSideMilvusException extends MilvusException {
  private final ErrorCode errorCode;
  private final String reason;

  public ServerSideMilvusException(String target, Status status) {
    super(target, false);
    this.errorCode = status.getErrorCode();
    this.reason = status.getReason();
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  @Override
  public String getErrorMessage() {
    return String.format("ServerSideMilvusException{errorCode=%s, reason=%s}", errorCode, reason);
  }
}
