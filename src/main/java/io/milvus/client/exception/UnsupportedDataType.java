package io.milvus.client.exception;

/** Milvus exception where unsupported DataType is used by client */
public class UnsupportedDataType extends ClientSideMilvusException {
  public UnsupportedDataType(String message) {
    super(null, message);
  }
}
