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
 * Bit flags describing the capabilities of a cluster in a global cluster deployment.
 */
public class ClusterCapability {
    /** The cluster can serve read requests. */
    public static final int READABLE = 0b01;
    /** The cluster can serve write requests. */
    public static final int WRITABLE = 0b10;
    /** The cluster is the primary, capable of both reads and writes. */
    public static final int PRIMARY = READABLE | WRITABLE; // 0b11

    private ClusterCapability() {
    }
}
