package io.milvus.client;

/** Thrown when client failed to connect to server */
public class ConnectFailedException extends Exception {

  public ConnectFailedException(String message) {
    super(message);
  }
}
