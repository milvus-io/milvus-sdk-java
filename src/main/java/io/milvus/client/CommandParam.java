package io.milvus.client;

import javax.annotation.Nonnull;

public class CommandParam {
    private final String command;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final String command;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        public Builder(@Nonnull String command) {
            this.command = command;
        }

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

    public String getCommand() {
        return command;
    }

    public long getTimeout() {
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
