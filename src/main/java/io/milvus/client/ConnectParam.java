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

import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/** Contains parameters for connecting to Milvus server */
public class ConnectParam {
  private final String target;
  private final String defaultLoadBalancingPolicy;
  private final long connectTimeoutNanos;
  private final long keepAliveTimeNanos;
  private final long keepAliveTimeoutNanos;
  private final boolean keepAliveWithoutCalls;
  private final long idleTimeoutNanos;

  private ConnectParam(@Nonnull Builder builder) {
    this.target =
        builder.target != null
            ? builder.target
            : String.format("dns:///%s:%d", builder.host, builder.port);
    this.defaultLoadBalancingPolicy = builder.defaultLoadBalancingPolicy;
    this.connectTimeoutNanos = builder.connectTimeoutNanos;
    this.keepAliveTimeNanos = builder.keepAliveTimeNanos;
    this.keepAliveTimeoutNanos = builder.keepAliveTimeoutNanos;
    this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
    this.idleTimeoutNanos = builder.idleTimeoutNanos;
  }

  public String getTarget() {
    return target;
  }

  public String getDefaultLoadBalancingPolicy() {
    return defaultLoadBalancingPolicy;
  }

  public long getConnectTimeout(@Nonnull TimeUnit timeUnit) {
    return timeUnit.convert(connectTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  public long getKeepAliveTime(@Nonnull TimeUnit timeUnit) {
    return timeUnit.convert(keepAliveTimeNanos, TimeUnit.NANOSECONDS);
  }

  public long getKeepAliveTimeout(@Nonnull TimeUnit timeUnit) {
    return timeUnit.convert(keepAliveTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  public boolean isKeepAliveWithoutCalls() {
    return keepAliveWithoutCalls;
  }

  public long getIdleTimeout(@Nonnull TimeUnit timeUnit) {
    return timeUnit.convert(idleTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  /** Builder for <code>ConnectParam</code> */
  public static class Builder {
    // Optional parameters - initialized to default values
    private String target = null;
    private String host = "localhost";
    private int port = 19530;
    private String defaultLoadBalancingPolicy = "round_robin";
    private long connectTimeoutNanos = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    private long keepAliveTimeNanos = Long.MAX_VALUE; // Disabling keepalive
    private long keepAliveTimeoutNanos = TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS);
    private boolean keepAliveWithoutCalls = false;
    private long idleTimeoutNanos = TimeUnit.NANOSECONDS.convert(24, TimeUnit.HOURS);

    /**
     * Optional. Defaults to null. Will be used in precedence to host and port.
     *
     * @param target a GRPC target string
     * @return <code>Builder</code>
     * @see ManagedChannelBuilder#forTarget(String)
     */
    public Builder withTarget(@Nonnull String target) {
      this.target = target;
      return this;
    }

    /**
     * Optional. Defaults to "localhost".
     *
     * @param host server host
     * @return <code>Builder</code>
     */
    public Builder withHost(@Nonnull String host) {
      this.host = host;
      return this;
    }

    /**
     * Optional. Defaults to "19530".
     *
     * @param port server port
     * @return <code>Builder</code>
     */
    public Builder withPort(int port) throws IllegalArgumentException {
      if (port < 0 || port > 0xFFFF) {
        throw new IllegalArgumentException("Port is out of range!");
      }
      this.port = port;
      return this;
    }

    /**
     * Optional. Defaults to "round_robin".
     *
     * @param defaultLoadBalancingPolicy the default load-balancing policy name
     * @return <code>Builder</code>
     */
    public Builder withDefaultLoadBalancingPolicy(String defaultLoadBalancingPolicy) {
      this.defaultLoadBalancingPolicy = defaultLoadBalancingPolicy;
      return this;
    }

    /**
     * Optional. Defaults to 10 seconds.
     *
     * @param connectTimeout Timeout for client to establish a connection to server
     * @return <code>Builder</code>
     * @throws IllegalArgumentException
     */
    public Builder withConnectTimeout(long connectTimeout, @Nonnull TimeUnit timeUnit)
        throws IllegalArgumentException {
      if (connectTimeout <= 0L) {
        throw new IllegalArgumentException("Connect timeout must be positive!");
      }
      connectTimeoutNanos = timeUnit.toNanos(connectTimeout);
      return this;
    }

    /**
     * Optional. Sets the time without read activity before sending a keepalive ping. An
     * unreasonably small value might be increased, and Long.MAX_VALUE nano seconds or an
     * unreasonably large value will disable keepalive. Defaults to infinite.
     *
     * @see <a
     *     href="https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-">
     *     GRPC keepAliveTime Javadoc</a>
     * @return <code>Builder</code>
     * @throws IllegalArgumentException
     */
    public Builder withKeepAliveTime(long keepAliveTime, @Nonnull TimeUnit timeUnit)
        throws IllegalArgumentException {
      if (keepAliveTime <= 0L) {
        throw new IllegalArgumentException("Keepalive time must be positive!");
      }
      keepAliveTimeNanos = timeUnit.toNanos(keepAliveTime);
      return this;
    }

    /**
     * Optional. Sets the time waiting for read activity after sending a keepalive ping. If the time
     * expires without any read activity on the connection, the connection is considered dead. An
     * unreasonably small value might be increased. Defaults to 20 seconds.
     *
     * <p>This value should be at least multiple times the RTT to allow for lost packets.
     *
     * @see <a
     *     href="https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveTimeout-long-java.util.concurrent.TimeUnit-">
     *     GRPC keepAliveTimeout Javadoc</a>
     * @return <code>Builder</code>
     * @throws IllegalArgumentException
     */
    public Builder withKeepAliveTimeout(long keepAliveTimeout, @Nonnull TimeUnit timeUnit)
        throws IllegalArgumentException {
      if (keepAliveTimeout <= 0L) {
        throw new IllegalArgumentException("Keepalive timeout must be positive!");
      }
      keepAliveTimeoutNanos = timeUnit.toNanos(keepAliveTimeout);
      return this;
    }

    /**
     * Optional. Sets whether keepalive will be performed when there are no outstanding RPC on a
     * connection. Defaults to false.
     *
     * @see <a
     *     href="https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveWithoutCalls-boolean-">
     *     GRPC keepAliveWithoutCalls Javadoc</a>
     * @return <code>Builder</code>
     */
    public Builder keepAliveWithoutCalls(boolean enable) {
      keepAliveWithoutCalls = enable;
      return this;
    }

    /**
     * Optional. Set the duration without ongoing RPCs before going to idle mode. A new RPC would
     * take the channel out of idle mode. Defaults to 24 hour.
     *
     * @see <a
     *     href="https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#idleTimeout-long-java.util.concurrent.TimeUnit-">
     *     GRPC idleTimeout Javadoc</a>
     * @return <code>Builder</code>
     * @throws IllegalArgumentException
     */
    public Builder withIdleTimeout(long idleTimeout, TimeUnit timeUnit)
        throws IllegalArgumentException {
      if (idleTimeout <= 0L) {
        throw new IllegalArgumentException("Idle timeout must be positive!");
      }
      idleTimeoutNanos = timeUnit.toNanos(idleTimeout);
      return this;
    }

    public ConnectParam build() {
      return new ConnectParam(this);
    }
  }
}
