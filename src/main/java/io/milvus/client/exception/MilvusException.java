package io.milvus.client.exception;

public class MilvusException extends RuntimeException {
  private boolean fillInStackTrace;

  MilvusException(boolean fillInStackTrace) {
    this.fillInStackTrace = fillInStackTrace;
  }

  MilvusException(boolean fillInStackTrace, Throwable cause) {
    super(cause);
    this.fillInStackTrace = fillInStackTrace;
  }

  MilvusException(boolean fillInStackTrace, String message) {
    super(message);
    this.fillInStackTrace = fillInStackTrace;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return fillInStackTrace ? super.fillInStackTrace() : this;
  }
}
