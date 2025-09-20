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

package io.milvus.param.highlevel.collection.response;

import java.util.List;

/**
 * Parameters for <code>showCollections</code> interface.
 */
public class ListCollectionsResponse {
    public List<String> collectionNames;

    private ListCollectionsResponse(Builder builder) {
        this.collectionNames = builder.collectionNames;
    }

    public static Builder builder() {
        return new Builder();
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "ListCollectionsResponse{" +
                "collectionNames=" + collectionNames +
                '}';
    }

    /**
     * Builder for {@link ListCollectionsResponse} class to replace @Builder annotation.
     */
    public static class Builder {
        private List<String> collectionNames;

        private Builder() {
        }

        public Builder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public ListCollectionsResponse build() {
            return new ListCollectionsResponse(this);
        }
    }
}
