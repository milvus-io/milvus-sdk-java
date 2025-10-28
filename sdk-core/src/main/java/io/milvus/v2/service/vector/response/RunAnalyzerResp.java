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

public class RunAnalyzerResp {
    private List<AnalyzerResult> results;

    private RunAnalyzerResp(RunAnalyzerRespBuilder builder) {
        this.results = builder.results;
    }

    public static RunAnalyzerRespBuilder builder() {
        return new RunAnalyzerRespBuilder();
    }

    public List<AnalyzerResult> getResults() {
        return results;
    }

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

        public RunAnalyzerRespBuilder results(List<AnalyzerResult> results) {
            this.results = results;
            return this;
        }

        public RunAnalyzerResp build() {
            return new RunAnalyzerResp(this);
        }
    }

    public static final class AnalyzerResult {
        private List<AnalyzerToken> tokens;

        private AnalyzerResult(AnalyzerResultBuilder builder) {
            this.tokens = builder.tokens;
        }

        public static AnalyzerResultBuilder builder() {
            return new AnalyzerResultBuilder();
        }

        public List<AnalyzerToken> getTokens() {
            return tokens;
        }

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

            public AnalyzerResultBuilder tokens(List<AnalyzerToken> tokens) {
                this.tokens = tokens;
                return this;
            }

            public AnalyzerResult build() {
                return new AnalyzerResult(this);
            }
        }
    }

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

        public static AnalyzerTokenBuilder builder() {
            return new AnalyzerTokenBuilder();
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public Long getStartOffset() {
            return startOffset;
        }

        public void setStartOffset(Long startOffset) {
            this.startOffset = startOffset;
        }

        public Long getEndOffset() {
            return endOffset;
        }

        public void setEndOffset(Long endOffset) {
            this.endOffset = endOffset;
        }

        public Long getPosition() {
            return position;
        }

        public void setPosition(Long position) {
            this.position = position;
        }

        public Long getPositionLength() {
            return positionLength;
        }

        public void setPositionLength(Long positionLength) {
            this.positionLength = positionLength;
        }

        public Long getHash() {
            return hash;
        }

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

            public AnalyzerTokenBuilder token(String token) {
                this.token = token;
                return this;
            }

            public AnalyzerTokenBuilder startOffset(Long startOffset) {
                this.startOffset = startOffset;
                return this;
            }

            public AnalyzerTokenBuilder endOffset(Long endOffset) {
                this.endOffset = endOffset;
                return this;
            }

            public AnalyzerTokenBuilder position(Long position) {
                this.position = position;
                return this;
            }

            public AnalyzerTokenBuilder positionLength(Long positionLength) {
                this.positionLength = positionLength;
                return this;
            }

            public AnalyzerTokenBuilder hash(Long hash) {
                this.hash = hash;
                return this;
            }

            public AnalyzerToken build() {
                return new AnalyzerToken(this);
            }
        }
    }
}
