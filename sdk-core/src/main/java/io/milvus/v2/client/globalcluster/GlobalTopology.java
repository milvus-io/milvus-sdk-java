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

import java.util.List;

public class GlobalTopology {
    private final long version;
    private final List<ClusterInfo> clusters;

    public GlobalTopology(long version, List<ClusterInfo> clusters) {
        this.version = version;
        this.clusters = clusters;
    }

    public long getVersion() {
        return version;
    }

    public List<ClusterInfo> getClusters() {
        return clusters;
    }

    public ClusterInfo getPrimary() {
        for (ClusterInfo cluster : clusters) {
            if (cluster.isPrimary()) {
                return cluster;
            }
        }
        throw new IllegalStateException("No primary (writable) cluster found in global topology");
    }

    @Override
    public String toString() {
        return "GlobalTopology{" +
                "version=" + version +
                ", clusters=" + clusters +
                '}';
    }
}
