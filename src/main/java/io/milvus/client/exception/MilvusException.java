package io.milvus.client.exception;

public abstract class MilvusException extends RuntimeException {
  private String target;
  private boolean fillInStackTrace;

  MilvusException(String target, boolean fillInStackTrace) {
    this(target, fillInStackTrace, null, null);
  }

  MilvusException(String target, boolean fillInStackTrace, String message, Throwable cause) {
    super(message, cause);
    this.target = target;
    this.fillInStackTrace = fillInStackTrace;
  }

  @Override
  public final String getMessage() {
    return String.format("%s: %s", target, getErrorMessage());
  }

  protected String getErrorMessage() {
    return super.getMessage();
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return fillInStackTrace ? super.fillInStackTrace() : this;
  }
}
