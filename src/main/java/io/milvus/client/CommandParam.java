/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

import javax.annotation.Nonnull;

/** Contains parameters for <code>command</code> */
class CommandParam {
  private final String command;
  private final long timeout;

  private CommandParam(@Nonnull Builder builder) {
    this.command = builder.command;
    this.timeout = builder.timeout;
  }

  String getCommand() {
    return command;
  }

  long getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return "CommandParam {" + "command='" + command + '\'' + ", timeout=" + timeout + '}';
  }

  /** Builder for <code>CommandParam</code> */
  public static class Builder {
    // Required parameters
    private final String command;

    // Optional parameters - initialized to default values
    private long timeout = 86400;

    /** @param command a string command */
    public Builder(@Nonnull String command) {
      this.command = command;
    }

    /**
     * Optional. Sets the deadline from when the client RPC is set to when the response is picked up
     * by the client. Default to 86400s (1 day).
     *
     * @param timeout in seconds
     * @return <code>Builder</code>
     */
    public Builder withTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public CommandParam build() {
      return new CommandParam(this);
    }
  }
}
