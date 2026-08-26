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

package io.milvus.v2.service.vector.response;

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code runAnalyzer} API.
 */
public class RunAnalyzerResp {
    private List<AnalyzerResult> results;

    private RunAnalyzerResp(RunAnalyzerRespBuilder builder) {
        this.results = builder.results;
    }

    /**
     * Creates a new {@code RunAnalyzerResp} builder.
     *
     * @return the builder
     */
    public static RunAnalyzerRespBuilder builder() {
        return new RunAnalyzerRespBuilder();
    }

    /**
     * Returns the analysis results, one per analyzed text.
     *
     * @return the analyzer results
     */
    public List<AnalyzerResult> getResults() {
        return results;
    }

    /**
     * Sets the analysis results.
     *
     * @param results the analyzer results
     */
    public void setResults(List<AnalyzerResult> results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return "RunAnalyzerResp{" +
                "results=" + results +
                '}';
    }

    public static class RunAnalyzerRespBuilder {
        private List<AnalyzerResult> results = new ArrayList<>();

        /**
         * Sets the analysis results.
         *
         * @param results the analyzer results
         * @return this builder
         */
        public RunAnalyzerRespBuilder results(List<AnalyzerResult> results) {
            this.results = results;
            return this;
        }

        /**
         * Builds the {@link RunAnalyzerResp}.
         *
         * @return the response
         */
        public RunAnalyzerResp build() {
            return new RunAnalyzerResp(this);
        }
    }

    /**
     * The analysis result of a single analyzed text.
     */
    public static final class AnalyzerResult {
        private List<AnalyzerToken> tokens;

        private AnalyzerResult(AnalyzerResultBuilder builder) {
            this.tokens = builder.tokens;
        }

        /**
         * Creates a new {@code AnalyzerResult} builder.
         *
         * @return the builder
         */
        public static AnalyzerResultBuilder builder() {
            return new AnalyzerResultBuilder();
        }

        /**
         * Returns the tokens produced by the analyzer.
         *
         * @return the analyzer tokens
         */
        public List<AnalyzerToken> getTokens() {
            return tokens;
        }

        /**
         * Sets the tokens produced by the analyzer.
         *
         * @param tokens the analyzer tokens
         */
        public void setTokens(List<AnalyzerToken> tokens) {
            this.tokens = tokens;
        }

        @Override
        public String toString() {
            return "AnalyzerResult{" +
                    "tokens=" + tokens +
                    '}';
        }

        public static class AnalyzerResultBuilder {
            private List<AnalyzerToken> tokens = new ArrayList<>();

            /**
             * Sets the tokens produced by the analyzer.
             *
             * @param tokens the analyzer tokens
             * @return this builder
             */
            public AnalyzerResultBuilder tokens(List<AnalyzerToken> tokens) {
                this.tokens = tokens;
                return this;
            }

            /**
             * Builds the {@link AnalyzerResult}.
             *
             * @return the analyzer result
             */
            public AnalyzerResult build() {
                return new AnalyzerResult(this);
            }
        }
    }

    /**
     * A single token produced by the analyzer.
     */
    public static final class AnalyzerToken {
        private String token;
        private Long startOffset;
        private Long endOffset;
        private Long position;
        private Long positionLength;
        private Long hash;

        private AnalyzerToken(AnalyzerTokenBuilder builder) {
            this.token = builder.token;
            this.startOffset = builder.startOffset;
            this.endOffset = builder.endOffset;
            this.position = builder.position;
            this.positionLength = builder.positionLength;
            this.hash = builder.hash;
        }

        /**
         * Creates a new {@code AnalyzerToken} builder.
         *
         * @return the builder
         */
        public static AnalyzerTokenBuilder builder() {
            return new AnalyzerTokenBuilder();
        }

        /**
         * Returns the token text.
         *
         * @return the token text
         */
        public String getToken() {
            return token;
        }

        /**
         * Sets the token text.
         *
         * @param token the token text
         */
        public void setToken(String token) {
            this.token = token;
        }

        /**
         * Returns the start offset of the token in the original text.
         *
         * @return the start offset
         */
        public Long getStartOffset() {
            return startOffset;
        }

        /**
         * Sets the start offset of the token in the original text.
         *
         * @param startOffset the start offset
         */
        public void setStartOffset(Long startOffset) {
            this.startOffset = startOffset;
        }

        /**
         * Returns the end offset of the token in the original text.
         *
         * @return the end offset
         */
        public Long getEndOffset() {
            return endOffset;
        }

        /**
         * Sets the end offset of the token in the original text.
         *
         * @param endOffset the end offset
         */
        public void setEndOffset(Long endOffset) {
            this.endOffset = endOffset;
        }

        /**
         * Returns the position of the token.
         *
         * @return the token position
         */
        public Long getPosition() {
            return position;
        }

        /**
         * Sets the position of the token.
         *
         * @param position the token position
         */
        public void setPosition(Long position) {
            this.position = position;
        }

        /**
         * Returns the position length of the token.
         *
         * @return the token position length
         */
        public Long getPositionLength() {
            return positionLength;
        }

        /**
         * Sets the position length of the token.
         *
         * @param positionLength the token position length
         */
        public void setPositionLength(Long positionLength) {
            this.positionLength = positionLength;
        }

        /**
         * Returns the hash value of the token.
         *
         * @return the token hash
         */
        public Long getHash() {
            return hash;
        }

        /**
         * Sets the hash value of the token.
         *
         * @param hash the token hash
         */
        public void setHash(Long hash) {
            this.hash = hash;
        }

        @Override
        public String toString() {
            return "AnalyzerToken{" +
                    "token='" + token + '\'' +
                    ", startOffset=" + startOffset +
                    ", endOffset=" + endOffset +
                    ", position=" + position +
                    ", positionLength=" + positionLength +
                    ", hash=" + hash +
                    '}';
        }

        public static class AnalyzerTokenBuilder {
            private String token;
            private Long startOffset;
            private Long endOffset;
            private Long position;
            private Long positionLength;
            private Long hash;

            /**
             * Sets the token text.
             *
             * @param token the token text
             * @return this builder
             */
            public AnalyzerTokenBuilder token(String token) {
                this.token = token;
                return this;
            }

            /**
             * Sets the start offset of the token in the original text.
             *
             * @param startOffset the start offset
             * @return this builder
             */
            public AnalyzerTokenBuilder startOffset(Long startOffset) {
                this.startOffset = startOffset;
                return this;
            }

            /**
             * Sets the end offset of the token in the original text.
             *
             * @param endOffset the end offset
             * @return this builder
             */
            public AnalyzerTokenBuilder endOffset(Long endOffset) {
                this.endOffset = endOffset;
                return this;
            }

            /**
             * Sets the position of the token.
             *
             * @param position the token position
             * @return this builder
             */
            public AnalyzerTokenBuilder position(Long position) {
                this.position = position;
                return this;
            }

            /**
             * Sets the position length of the token.
             *
             * @param positionLength the token position length
             * @return this builder
             */
            public AnalyzerTokenBuilder positionLength(Long positionLength) {
                this.positionLength = positionLength;
                return this;
            }

            /**
             * Sets the hash value of the token.
             *
             * @param hash the token hash
             * @return this builder
             */
            public AnalyzerTokenBuilder hash(Long hash) {
                this.hash = hash;
                return this;
            }

            /**
             * Builds the {@link AnalyzerToken}.
             *
             * @return the analyzer token
             */
            public AnalyzerToken build() {
                return new AnalyzerToken(this);
            }
        }
    }
}
