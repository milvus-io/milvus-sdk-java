package io.milvus.client.exception;

public class InitializationException extends ClientSideMilvusException {
  public InitializationException(String target, String message) {
    super(target, message);
  }
}
