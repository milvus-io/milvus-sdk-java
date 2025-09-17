package io.milvus.pool;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

@Data
@SuperBuilder
public class PoolConfig {
    @Builder.Default
    private int maxIdlePerKey = 10;
    @Builder.Default
    private int minIdlePerKey = 0;
    @Builder.Default
    private int maxTotalPerKey = 50;
    @Builder.Default
    private int maxTotal = 1000;
    @Builder.Default
    private boolean blockWhenExhausted = true;
    @Builder.Default
    private Duration maxBlockWaitDuration = Duration.ofSeconds(3L);
    @Builder.Default
    private Duration evictionPollingInterval = Duration.ofSeconds(60L);
    @Builder.Default
    private Duration minEvictableIdleDuration = Duration.ofSeconds(10L);
    @Builder.Default
    private boolean testOnBorrow = false;
    @Builder.Default
    private boolean testOnReturn = true;
}
