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

package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.grpc.ShowType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Params for ShowCollections RPC operation
 *
 * @author changzechuan
 */
@Getter
public class ShowCollectionsParam {
    private final List<String> collectionNames;
    private final ShowType showType;

    private ShowCollectionsParam(@NonNull Builder builder) {
        this.collectionNames = builder.collectionNames;
        this.showType = builder.showType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> collectionNames = new ArrayList<>();
        // showType:
        //   default showType = ShowType.All
        //   if collectionNames is not empty, set showType = ShowType.InMemory
        private ShowType showType = ShowType.All;

        private Builder() {
        }

        public Builder withCollectionNames(@NonNull List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public Builder addCollectionName(@NonNull String collectionName) {
            this.collectionNames.add(collectionName);
            return this;
        }

        public ShowCollectionsParam build() throws ParamException {
            if (collectionNames != null && !collectionNames.isEmpty()) {
                for (String collectionName : collectionNames) {
                    ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
                }
                this.showType = ShowType.InMemory;
            }

            return new ShowCollectionsParam(this);
        }
    }

    @Override
    public String toString() {
        return "ShowCollectionsParam{" +
                "collectionNames='" + collectionNames.toString() + '\'' +
                ", showType=" + showType.toString() +
                '}';
    }
}
