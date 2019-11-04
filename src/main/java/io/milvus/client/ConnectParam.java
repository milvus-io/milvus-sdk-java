/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.client;

import javax.annotation.Nonnull;

/** Contains parameters for connecting to Milvus server */
public class ConnectParam {
  private final String host;
  private final String port;
  private final long timeout;

  private ConnectParam(@Nonnull Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.timeout = builder.timeout;
  }

  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public long getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return "ConnectParam{" + "host='" + host + '\'' + ", port='" + port + '\'' + '}';
  }

  /** Builder for <code>ConnectParam</code> */
  public static class Builder {
    // Optional parameters - initialized to default values
    private String host = "127.0.0.1";
    private String port = "19530";
    private long timeout = 10000; // ms

    /**
     * Optional. Default to "127.0.0.1".
     *
     * @param host server host
     * @return <code>Builder</code>
     */
    public Builder withHost(@Nonnull String host) {
      this.host = host;
      return this;
    }

    /**
     * Optional. Default to "19530".
     *
     * @param port server port
     * @return <code>Builder</code>
     */
    public Builder withPort(@Nonnull String port) {
      this.port = port;
      return this;
    }

    /**
     * Optional. Default to 10000 ms
     *
     * @param timeout Timeout in ms for client to establish a connection to server
     * @return <code>Builder</code>
     */
    public Builder withTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public ConnectParam build() {
      return new ConnectParam(this);
    }
  }
}
