package io.milvus.connection;

import io.milvus.exception.ParamException;
import io.milvus.param.QueryNodeSingleSearch;
import io.milvus.param.ServerAddress;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Factory with managing multi cluster.
 */
public class ClusterFactory {

    private final List<ServerSetting> serverSettings;

    private ServerSetting master;

    private List<ServerSetting> availableServerSettings;

    private ServerMonitor monitor;

    private ClusterFactory(@NonNull Builder builder) {
        this.serverSettings = builder.serverSettings;
        this.master = this.getDefaultServer();
        this.availableServerSettings = builder.serverSettings;
        if (builder.keepMonitor) {
            monitor = new ServerMonitor(this, builder.queryNodeSingleSearch);
            monitor.start();
        }
    }

    public ServerSetting getDefaultServer() {
        return serverSettings.get(0);
    }

    public boolean masterIsRunning() {
        List<ServerAddress> serverAddresses = availableServerSettings.stream()
                .map(ServerSetting::getServerAddress)
                .collect(Collectors.toList());

        return serverAddresses.contains(master.getServerAddress());
    }

    public void masterChange(ServerSetting serverSetting) {
        this.master = serverSetting;
    }

    public void availableServerChange(List<ServerSetting> serverSettings) {
        this.availableServerSettings = serverSettings;
    }

    public ServerSetting electMaster() {
        return CollectionUtils.isNotEmpty(availableServerSettings) ? availableServerSettings.get(0) : getDefaultServer();
    }

    public void close() {
        if (null != monitor) {
            monitor.close();
        }
    }

    public List<ServerSetting> getServerSettings() {
        return serverSettings;
    }

    public ServerSetting getMaster() {
        return master;
    }

    public List<ServerSetting> getAvailableServerSettings() {
        return availableServerSettings;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ClusterFactory}
     */
    public static class Builder {
        private List<ServerSetting> serverSettings;
        private boolean keepMonitor = false;
        private QueryNodeSingleSearch queryNodeSingleSearch;

        private Builder() {
        }

        /**
         * Sets server setting list
         *
         * @param serverSettings ServerSetting
         * @return <code>Builder</code>
         */
        public Builder withServerSetting(@NonNull List<ServerSetting> serverSettings) {
            this.serverSettings = serverSettings;
            return this;
        }

        /**
         * Enables the keep-monitor function for server
         *
         * @param enable true keep-monitor
         * @return <code>Builder</code>
         */
        public Builder keepMonitor(boolean enable) {
            this.keepMonitor = enable;
            return this;
        }

        /**
         * Sets single search for query node listener.
         *
         * @param queryNodeSingleSearch query node single search for listener
         * @return <code>Builder</code>
         */
        public Builder withQueryNodeSingleSearch(QueryNodeSingleSearch queryNodeSingleSearch) {
            this.queryNodeSingleSearch = queryNodeSingleSearch;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ClusterFactory} instance.
         *
         * @return {@link ClusterFactory}
         */
        public ClusterFactory build() throws ParamException {

            if (CollectionUtils.isEmpty(serverSettings)) {
                throw new ParamException("Server settings is empty!");
            }

            return new ClusterFactory(this);
        }
    }
}
