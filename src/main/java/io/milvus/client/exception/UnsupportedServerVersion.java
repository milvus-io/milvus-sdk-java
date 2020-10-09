package io.milvus.client.exception;

import io.milvus.client.MilvusClient;

public class UnsupportedServerVersion extends MilvusException {
  private String host;
  private String expect;
  private String actual;

  public UnsupportedServerVersion(String host, String expect, String actual) {
    super(false);
    this.host = host;
    this.expect = expect;
    this.actual = actual;
  }

  @Override
  public String getMessage() {
    return String.format("%s: Milvus client %s is expected to work with Milvus server %s, but the version of the connected server is %s",
        host, MilvusClient.clientVersion, expect, actual);
  }
}