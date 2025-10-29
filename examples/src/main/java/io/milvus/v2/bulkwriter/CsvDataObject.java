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

package io.milvus.v2.bulkwriter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.List;

public class CsvDataObject {
    private static final Gson GSON_INSTANCE = new Gson();

    @JsonProperty
    private String vector;
    @JsonProperty
    private String path;
    @JsonProperty
    private String label;

    public String getVector() {
        return vector;
    }

    public String getPath() {
        return path;
    }

    public String getLabel() {
        return label;
    }

    public List<Float> toFloatArray() {
        return GSON_INSTANCE.fromJson(vector, new TypeToken<List<Float>>() {
        }.getType());
    }
}
