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

package io.milvus.v2.client.globalcluster;

/**
 * A single ha-manager seed server resolved from a {@code _grpc._tcp.<hostname>} SRV record set.
 * <p>
 * Lower {@code priority} is nearer; {@code weight} breaks ties between equal priorities.
 */
final class SrvTarget {
    final int priority;
    final int weight;
    final int port;
    final String target;

    SrvTarget(int priority, int weight, int port, String target) {
        this.priority = priority;
        this.weight = weight;
        this.port = port;
        this.target = target;
    }

    @Override
    public String toString() {
        return target + ":" + port + " (priority=" + priority + ", weight=" + weight + ")";
    }
}
