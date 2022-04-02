package io.milvus.param;

import io.milvus.exception.ParamException;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parameters for client connection of multi server.
 */
public class MultiConnectParam {
    private final List<ServerAddress> hosts;
    private final QueryNodeSingleSearch queryNodeSingleSearch;
    private final long connectTimeoutMs;
    private final long keepAliveTimeMs;
    private final long keepAliveTimeoutMs;
    private final boolean keepAliveWithoutCalls;
    private final long idleTimeoutMs;

    private MultiConnectParam(@NonNull Builder builder) {
        this.hosts = builder.hosts;
        this.queryNodeSingleSearch = builder.queryNodeSingleSearch;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.keepAliveTimeMs = builder.keepAliveTimeMs;
        this.keepAliveTimeoutMs = builder.keepAliveTimeoutMs;
        this.keepAliveWithoutCalls = builder.keepAliveWithoutCalls;
        this.idleTimeoutMs = builder.idleTimeoutMs;
    }

    public List<ServerAddress> getHosts() {
        return hosts;
    }

    public QueryNodeSingleSearch getQueryNodeSingleSearch() {
        return queryNodeSingleSearch;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public long getKeepAliveTimeMs() {
        return keepAliveTimeMs;
    }

    public long getKeepAliveTimeoutMs() {
        return keepAliveTimeoutMs;
    }

    public boolean isKeepAliveWithoutCalls() {
        return keepAliveWithoutCalls;
    }

    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link MultiConnectParam}
     */
    public static class Builder {
        private List<ServerAddress> hosts;
        private QueryNodeSingleSearch queryNodeSingleSearch;
        private long connectTimeoutMs = 10000;
        private long keepAliveTimeMs = Long.MAX_VALUE; // Disabling keep alive
        private long keepAliveTimeoutMs = 20000;
        private boolean keepAliveWithoutCalls = false;
        private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);

        private Builder() {
        }

        /**
         * Sets the addresses.
         *
         * @param hosts hosts serverAddresses
         * @return <code>Builder</code>
         */
        public Builder withHosts(@NonNull List<ServerAddress> hosts) {
            this.hosts = hosts;
            return this;
        }

        /**
         * Sets single search for query node listener.
         *
         * @param queryNodeSingleSearch query node single search for listener
         * @return <code>Builder</code>
         */
        public Builder withQueryNodeSingleSearch(@NonNull QueryNodeSingleSearch queryNodeSingleSearch) {
            this.queryNodeSingleSearch = queryNodeSingleSearch;
            return this;
        }

        /**
         * Sets the connection timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param connectTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withConnectTimeout(long connectTimeout, @NonNull TimeUnit timeUnit) {
            this.connectTimeoutMs = timeUnit.toMillis(connectTimeout);
            return this;
        }

        /**
         * Sets the keep-alive time value of client channel. The keep-alive value must be greater than zero.
         *
         * @param keepAliveTime keep-alive value
         * @param timeUnit keep-alive unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTime(long keepAliveTime, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeMs = timeUnit.toMillis(keepAliveTime);
            return this;
        }

        /**
         * Sets the keep-alive timeout value of client channel. The timeout value must be greater than zero.
         *
         * @param keepAliveTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withKeepAliveTimeout(long keepAliveTimeout, @NonNull TimeUnit timeUnit) {
            this.keepAliveTimeoutMs = timeUnit.toNanos(keepAliveTimeout);
            return this;
        }

        /**
         * Enables the keep-alive function for client channel.
         *
         * @param enable true keep-alive
         * @return <code>Builder</code>
         */
        public Builder keepAliveWithoutCalls(boolean enable) {
            keepAliveWithoutCalls = enable;
            return this;
        }

        /**
         * Sets the idle timeout value of client channel. The timeout value must be larger than zero.
         *
         * @param idleTimeout timeout value
         * @param timeUnit timeout unit
         * @return <code>Builder</code>
         */
        public Builder withIdleTimeout(long idleTimeout, @NonNull TimeUnit timeUnit) {
            this.idleTimeoutMs = timeUnit.toMillis(idleTimeout);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link MultiConnectParam} instance.
         *
         * @return {@link MultiConnectParam}
         */
        public MultiConnectParam build() throws ParamException {
            if (CollectionUtils.isEmpty(hosts)) {
                throw new ParamException("Server addresses is empty!");
            }

            for (ServerAddress serverAddress : hosts) {
                ParamUtils.CheckNullEmptyString(serverAddress.getHost(), "Host name");

                if (serverAddress.getPort() < 0 || serverAddress.getPort() > 0xFFFF) {
                    throw new ParamException("Port is out of range!");
                }
            }

            if (keepAliveTimeMs <= 0L) {
                throw new ParamException("Keep alive time must be positive!");
            }

            if (connectTimeoutMs <= 0L) {
                throw new ParamException("Connect timeout must be positive!");
            }

            if (keepAliveTimeoutMs <= 0L) {
                throw new ParamException("Keep alive timeout must be positive!");
            }

            if (idleTimeoutMs <= 0L) {
                throw new ParamException("Idle timeout must be positive!");
            }

            return new MultiConnectParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link ConnectParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MultiConnectParam{");
        sb.append("hosts=").append(hosts);
        sb.append('}');
        return sb.toString();
    }
}
