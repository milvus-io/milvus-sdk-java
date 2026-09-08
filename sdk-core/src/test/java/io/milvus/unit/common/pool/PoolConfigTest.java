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

package io.milvus.unit.common.pool;

import io.milvus.pool.PoolConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@Tag("unit")
class PoolConfigTest {

    @Test
    void builderAppliesDocumentedDefaults() {
        PoolConfig config = PoolConfig.builder().build();
        Assertions.assertEquals(1, config.getMinIdlePerKey());
        Assertions.assertEquals(2, config.getMaxIdlePerKey());
        Assertions.assertEquals(5, config.getMaxTotalPerKey());
        Assertions.assertEquals(1000, config.getMaxTotal());
        Assertions.assertTrue(config.isBlockWhenExhausted());
        Assertions.assertEquals(Duration.ofSeconds(3L), config.getMaxBlockWaitDuration());
        Assertions.assertEquals(Duration.ofSeconds(60L), config.getEvictionPollingInterval());
        Assertions.assertEquals(Duration.ofSeconds(10L), config.getMinEvictableIdleDuration());
        Assertions.assertFalse(config.isTestOnBorrow());
        Assertions.assertTrue(config.isTestOnReturn());
    }

    @Test
    void builderSettersRoundTrip() {
        PoolConfig config = PoolConfig.builder()
                .minIdlePerKey(3)
                .maxIdlePerKey(7)
                .maxTotalPerKey(11)
                .maxTotal(500)
                .blockWhenExhausted(false)
                .maxBlockWaitDuration(Duration.ofMillis(100L))
                .evictionPollingInterval(Duration.ofSeconds(5L))
                .minEvictableIdleDuration(Duration.ofSeconds(1L))
                .testOnBorrow(true)
                .testOnReturn(false)
                .build();
        Assertions.assertEquals(3, config.getMinIdlePerKey());
        Assertions.assertEquals(7, config.getMaxIdlePerKey());
        Assertions.assertEquals(11, config.getMaxTotalPerKey());
        Assertions.assertEquals(500, config.getMaxTotal());
        Assertions.assertFalse(config.isBlockWhenExhausted());
        Assertions.assertEquals(Duration.ofMillis(100L), config.getMaxBlockWaitDuration());
        Assertions.assertEquals(Duration.ofSeconds(5L), config.getEvictionPollingInterval());
        Assertions.assertEquals(Duration.ofSeconds(1L), config.getMinEvictableIdleDuration());
        Assertions.assertTrue(config.isTestOnBorrow());
        Assertions.assertFalse(config.isTestOnReturn());
    }

    @Test
    void builderMethodsAreChainable() {
        PoolConfig.Builder builder = PoolConfig.builder();
        Assertions.assertSame(builder, builder.minIdlePerKey(1));
        Assertions.assertSame(builder, builder.maxIdlePerKey(1));
        Assertions.assertSame(builder, builder.maxTotalPerKey(1));
        Assertions.assertSame(builder, builder.maxTotal(1));
        Assertions.assertSame(builder, builder.blockWhenExhausted(true));
        Assertions.assertSame(builder, builder.maxBlockWaitDuration(Duration.ZERO));
        Assertions.assertSame(builder, builder.evictionPollingInterval(Duration.ZERO));
        Assertions.assertSame(builder, builder.minEvictableIdleDuration(Duration.ZERO));
        Assertions.assertSame(builder, builder.testOnBorrow(true));
        Assertions.assertSame(builder, builder.testOnReturn(true));
    }

    @Test
    void settersRoundTrip() {
        PoolConfig config = PoolConfig.builder().build();
        config.setMinIdlePerKey(9);
        config.setMaxIdlePerKey(10);
        config.setMaxTotalPerKey(20);
        config.setMaxTotal(300);
        config.setBlockWhenExhausted(false);
        config.setMaxBlockWaitDuration(Duration.ofSeconds(2L));
        config.setEvictionPollingInterval(Duration.ofSeconds(30L));
        config.setMinEvictableIdleDuration(Duration.ofSeconds(4L));
        config.setTestOnBorrow(true);
        config.setTestOnReturn(false);

        Assertions.assertEquals(9, config.getMinIdlePerKey());
        Assertions.assertEquals(10, config.getMaxIdlePerKey());
        Assertions.assertEquals(20, config.getMaxTotalPerKey());
        Assertions.assertEquals(300, config.getMaxTotal());
        Assertions.assertFalse(config.isBlockWhenExhausted());
        Assertions.assertEquals(Duration.ofSeconds(2L), config.getMaxBlockWaitDuration());
        Assertions.assertEquals(Duration.ofSeconds(30L), config.getEvictionPollingInterval());
        Assertions.assertEquals(Duration.ofSeconds(4L), config.getMinEvictableIdleDuration());
        Assertions.assertTrue(config.isTestOnBorrow());
        Assertions.assertFalse(config.isTestOnReturn());
    }

    @Test
    void nullDurationsRejectedByBuilder() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PoolConfig.builder().maxBlockWaitDuration(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PoolConfig.builder().evictionPollingInterval(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PoolConfig.builder().minEvictableIdleDuration(null));
    }

    @Test
    void toStringContainsAllFields() {
        String s = PoolConfig.builder().build().toString();
        Assertions.assertTrue(s.contains("maxIdlePerKey=2"));
        Assertions.assertTrue(s.contains("minIdlePerKey=1"));
        Assertions.assertTrue(s.contains("maxTotalPerKey=5"));
        Assertions.assertTrue(s.contains("maxTotal=1000"));
        Assertions.assertTrue(s.contains("blockWhenExhausted=true"));
        Assertions.assertTrue(s.contains("maxBlockWaitDuration=PT3S"));
        Assertions.assertTrue(s.contains("evictionPollingInterval=PT1M"));
        Assertions.assertTrue(s.contains("minEvictableIdleDuration=PT10S"));
        Assertions.assertTrue(s.contains("testOnBorrow=false"));
        Assertions.assertTrue(s.contains("testOnReturn=true"));
    }
}
