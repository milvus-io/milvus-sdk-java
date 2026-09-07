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

package io.milvus.unit.v2.client.globalcluster;

import io.milvus.v2.client.globalcluster.GlobalTopology;
import io.milvus.v2.client.globalcluster.TopologyRefresher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TopologyRefresherTest {

    private static TopologyRefresher refresher() {
        return new TopologyRefresher("https://host.global-cluster.example:443", "token", 1L,
                (GlobalTopology topology) -> { });
    }

    private static AtomicBoolean refreshingField(TopologyRefresher refresher) throws Exception {
        Field field = TopologyRefresher.class.getDeclaredField("refreshing");
        field.setAccessible(true);
        return (AtomicBoolean) field.get(refresher);
    }

    @Test
    void constructorAcceptsConsumerAndStartDoesNotThrow() {
        TopologyRefresher refresher = refresher();

        refresher.start();
        refresher.stop();
    }

    @Test
    void triggerRefreshSkipsWhenRefreshAlreadyInProgress() throws Exception {
        TopologyRefresher refresher = refresher();
        AtomicBoolean refreshing = refreshingField(refresher);
        refreshing.set(true);

        refresher.triggerRefresh();

        assertTrue(refreshing.get());
        refresher.stop();
    }

    @Test
    void triggerRefreshAfterStopDoesNotThrowAndResetsFlag() throws Exception {
        TopologyRefresher refresher = refresher();
        refresher.stop();

        refresher.triggerRefresh();

        assertFalse(refreshingField(refresher).get());
    }

    @Test
    void stopIsIdempotent() {
        TopologyRefresher refresher = refresher();
        refresher.stop();
        refresher.stop();
    }
}
