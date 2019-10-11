package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for <code>command</code>
 */
class CommandParam {
    private final String command;
    private final long timeout;

    /**
     * Builder for <code>CommandParam</code>
     */
    public static class Builder {
        // Required parameters
        private final String command;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        /**
         * @param command a string command
         */
        public Builder(@Nonnull String command) {
            this.command = command;
        }

         /**
         * Optional. Sets the deadline from when the client RPC is set to when the response is picked up by the client.
         * Default to 86400s (1 day).
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
        return "CommandParam {" +
                "command='" + command + '\'' +
                ", timeout=" + timeout +
                '}';
    }
}
