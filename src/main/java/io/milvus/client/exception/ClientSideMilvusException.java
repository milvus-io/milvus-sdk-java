package io.milvus.client.exception;

public class ClientSideMilvusException extends MilvusException {

  public ClientSideMilvusException(String target) {
    super(target, false, null, null);
  }

  public ClientSideMilvusException(String target, Throwable cause) {
    super(target, false, null, cause);
  }

  public ClientSideMilvusException(String target, String message) {
    super(target, false, message, null);
  }
}
