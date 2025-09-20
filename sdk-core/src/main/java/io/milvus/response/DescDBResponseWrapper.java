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

package io.milvus.response;

import io.milvus.grpc.*;
import io.milvus.param.Constant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Util class to wrap response of <code>describeCollection</code> interface.
 */
public class DescDBResponseWrapper {
    private final DescribeDatabaseResponse response;
    Map<String, String> pairs = new HashMap<>();

    public DescDBResponseWrapper(DescribeDatabaseResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("DescribeDatabaseResponse cannot be null");
        }
        this.response = response;
        response.getPropertiesList().forEach((prop) -> pairs.put(prop.getKey(), prop.getValue()));
    }


    /**
     * get database name
     *
     * @return database name
     */
    public String getDatabaseName() {
        return response.getDbName();
    }

    /**
     * Get properties of the collection.
     *
     * @return map of key value pair
     */
    public Map<String, String> getProperties() {
        return pairs;
    }

    /**
     * return database resource groups
     *
     * @return resource group names
     */
    public List<String> getResourceGroups() {
        String value = pairs.get(Constant.DATABASE_RESOURCE_GROUPS);
        if (value == null) {
            return new ArrayList<>();
        }
        return Arrays.asList(value.split(","));
    }

    /**
     * return database replica number
     *
     * @return database replica number
     */
    public int getReplicaNumber() {
        String value = pairs.get(Constant.DATABASE_REPLICA_NUMBER);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(pairs.get(Constant.DATABASE_REPLICA_NUMBER));
    }

    /**
     * Construct a <code>String</code> by {@link DescCollResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Database Description{" +
            "name:'" + getDatabaseName() + '\'' +
            ", properties:" + getProperties() +
            '}';
    }
}
