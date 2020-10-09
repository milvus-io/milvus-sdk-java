package io.milvus.client.exception;

public class InitializationException extends MilvusException {
  public InitializationException(String host, String message) {
    super(false, host + ": " + message);
  }
}
