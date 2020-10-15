package io.milvus.client.exception;

public class UnsupportedDataType extends ClientSideMilvusException {
  public UnsupportedDataType(String message) {
    super(null, message);
  }
}
