package io.milvus.client.exception;

public class InitializationException extends MilvusException {
  private String host;
  private Throwable cause;

  public InitializationException(String host, Throwable cause) {
    super(false, cause);
    this.host = host;
  }

  public InitializationException(String host, String message) {
    super(false, message);
    this.host = host;
  }
}
