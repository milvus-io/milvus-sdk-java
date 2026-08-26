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

package io.milvus.v2.service.vector.request.data;

import io.milvus.grpc.PlaceholderType;

/**
 * An embedded text used for searching text stored in a struct field of a collection,
 * where the text is embedded on the server side during the search.
 */
public class EmbeddedText implements BaseVector {
    private final String data;

    /**
     * Constructs an embedded text from a string.
     *
     * @param data the text data
     */
    public EmbeddedText(String data) {
        this.data = data;
    }

    /**
     * Returns the placeholder type of an embedded text.
     *
     * @return the {@link PlaceholderType#VarChar} placeholder type
     */
    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.VarChar;
    }

    /**
     * Returns the embedded text data.
     *
     * @return the text data as a string
     */
    @Override
    public Object getData() {
        return this.data;
    }
}
