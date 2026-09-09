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

package io.milvus.unit.v1.connection;

import io.milvus.connection.ClusterListener;
import io.milvus.connection.Listener;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ClusterListenerTest {

    @Test
    void testConstructionImplementsListener() {
        ClusterListener listener = new ClusterListener();
        assertNotNull(listener);
        assertTrue(listener instanceof Listener);
    }

    @Test
    void testHeartbeatRejectsNullServerSetting() {
        ClusterListener listener = new ClusterListener();
        // heartBeat() dereferences serverSetting.getServerAddress() before its
        // internal try/catch, so a null argument propagates immediately.
        assertThrows(NullPointerException.class, () -> listener.heartBeat(null));
    }
}
