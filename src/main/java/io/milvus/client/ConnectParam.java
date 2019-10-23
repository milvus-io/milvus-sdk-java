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
  private final long waitTime;

  private ConnectParam(@Nonnull Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.waitTime = builder.waitTime;
  }

  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public long getWaitTime() {
    return waitTime;
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
    private long waitTime = 500; // ms

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
     * Optional. Default to 500 ms
     *
     * @param waitTime Wait time (in ms) for channel to establish a connection. BEWARE: this is not
     *     timeout, so the program will essentially sleep for the entire duration
     * @return <code>Builder</code>
     */
    public Builder withWaitTime(long waitTime) {
      this.waitTime = waitTime;
      return this;
    }

    public ConnectParam build() {
      return new ConnectParam(this);
    }
  }
}
