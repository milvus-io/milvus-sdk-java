package io.milvus.client.exception;

public class InitializationFailedException extends MilvusException {
  private String host;
  private Throwable cause;

  public InitializationFailedException(String host, Throwable cause) {
    super(false, cause);
    this.host = host;
  }
}
