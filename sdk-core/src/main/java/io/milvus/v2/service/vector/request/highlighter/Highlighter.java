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

package io.milvus.v2.service.vector.request.highlighter;

import java.util.Map;

/**
 * A highlighter used by the {@code search} API to mark the matched terms in the returned
 * text fields. Implementations include the {@link LexicalHighlighter} and the
 * {@link SemanticHighlighter}.
 */
public interface Highlighter {
    /**
     * Returns the highlight type name, for example {@code Lexical} or {@code Semantic}.
     *
     * @return the highlight type name
     */
    String highlightType();

    /**
     * Returns the highlight parameters as a map of parameter name to value.
     *
     * @return the highlight parameters
     */
    Map<String, String> getParams();
}
