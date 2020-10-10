package io.milvus.client.exception;

public class InvalidDsl extends ClientSideMilvusException {
  private String dsl;

  public InvalidDsl(String dsl, String message) {
    super(null, message);
    this.dsl = dsl;
  }

  @Override
  protected String getErrorMessage() {
    return super.getErrorMessage() + ": " + dsl;
  }
}
