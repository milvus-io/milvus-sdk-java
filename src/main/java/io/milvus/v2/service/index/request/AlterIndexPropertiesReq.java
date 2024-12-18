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

package io.milvus.v2.service.index.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder
public class AlterIndexPropertiesReq {
    private String collectionName;
    private String databaseName;
    private String indexName;
    @Builder.Default
    private Map<String, String> properties = new HashMap<>();


    public static abstract class AlterIndexPropertiesReqBuilder<C extends AlterIndexPropertiesReq, B extends AlterIndexPropertiesReq.AlterIndexPropertiesReqBuilder<C, B>> {
        public B property(String key, String value) {
            if(null == this.properties$value ){
                this.properties$value = new HashMap<>();
            }
            this.properties$value.put(key, value);
            this.properties$set = true;
            return self();
        }
    }
}
