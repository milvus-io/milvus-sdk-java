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

package io.milvus.bulkwriter.common.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for normalizing field values before they are written to bulk data files.
 */
public class WriterUtils {
    private WriterUtils() {
    }

    /**
     * Normalizes a field value so that it can be serialized into a bulk data file. {@link ByteBuffer}
     * values are converted into a list of unsigned byte integers, and lists and maps are normalized
     * recursively. Any other value is returned unchanged.
     *
     * @param value the field value to normalize
     * @return the normalized field value
     */
    public static Object normalizeValue(Object value) {
        if (value instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) value;
            byte[] bytes = byteBuffer.array();
            List<Integer> result = new ArrayList<>(bytes.length);
            for (byte b : bytes) {
                result.add((int) b);
            }
            return result;
        }
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            List<Object> normalized = null;
            for (int i = 0; i < values.size(); i++) {
                Object item = values.get(i);
                Object normalizedItem = normalizeValue(item);
                if (normalized != null) {
                    normalized.add(normalizedItem);
                    continue;
                }
                if (normalizedItem != item) {
                    normalized = new ArrayList<>(values.size());
                    for (int j = 0; j < i; j++) {
                        normalized.add(values.get(j));
                    }
                    normalized.add(normalizedItem);
                }
            }
            return normalized != null ? normalized : value;
        }
        if (value instanceof Map) {
            Map<Object, Object> values = (Map<Object, Object>) value;
            values.replaceAll((k, v) -> normalizeValue(v));
            return values;
        }
        return value;
    }
}
