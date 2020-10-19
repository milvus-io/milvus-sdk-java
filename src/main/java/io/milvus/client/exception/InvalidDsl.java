package io.milvus.client.exception;

/** Milvus exception where invalid DSL is passed by client as a query */
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
