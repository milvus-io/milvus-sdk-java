package io.milvus.client.exception;

import io.milvus.client.MilvusClient;

/** Milvus exception where client and server versions do not match */
public class UnsupportedServerVersion extends ClientSideMilvusException {
  private final String expect;
  private final String actual;

  public UnsupportedServerVersion(String target, String expect, String actual) {
    super(target);
    this.expect = expect;
    this.actual = actual;
  }

  @Override
  public String getErrorMessage() {
    return String.format(
        "Milvus client %s is expected to work with Milvus server %s, but the version of the connected server is %s",
        MilvusClient.clientVersion, expect, actual);
  }
}
