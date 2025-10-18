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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

public class RunAnalyzerResp {
    private List<AnalyzerResult> results;

    private RunAnalyzerResp(Builder builder) {
        this.results = builder.results;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<AnalyzerResult> getResults() {
        return results;
    }

    public void setResults(List<AnalyzerResult> results) {
        this.results = results;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RunAnalyzerResp that = (RunAnalyzerResp) obj;
        return new EqualsBuilder()
                .append(results, that.results)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(results)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "RunAnalyzerResp{" +
                "results=" + results +
                '}';
    }

    public static class Builder {
        private List<AnalyzerResult> results = new ArrayList<>();

        public Builder results(List<AnalyzerResult> results) {
            this.results = results;
            return this;
        }

        public RunAnalyzerResp build() {
            return new RunAnalyzerResp(this);
        }
    }

    public static final class AnalyzerResult {
        private List<AnalyzerToken> tokens;

        private AnalyzerResult(Builder builder) {
            this.tokens = builder.tokens;
        }

        public static Builder builder() {
            return new Builder();
        }

        public List<AnalyzerToken> getTokens() {
            return tokens;
        }

        public void setTokens(List<AnalyzerToken> tokens) {
            this.tokens = tokens;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            AnalyzerResult that = (AnalyzerResult) obj;
            return new EqualsBuilder()
                    .append(tokens, that.tokens)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(tokens)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "AnalyzerResult{" +
                    "tokens=" + tokens +
                    '}';
        }

        public static class Builder {
            private List<AnalyzerToken> tokens = new ArrayList<>();

            public Builder tokens(List<AnalyzerToken> tokens) {
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

        private AnalyzerToken(Builder builder) {
            this.token = builder.token;
            this.startOffset = builder.startOffset;
            this.endOffset = builder.endOffset;
            this.position = builder.position;
            this.positionLength = builder.positionLength;
            this.hash = builder.hash;
        }

        public static Builder builder() {
            return new Builder();
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
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            AnalyzerToken that = (AnalyzerToken) obj;
            return new EqualsBuilder()
                    .append(token, that.token)
                    .append(startOffset, that.startOffset)
                    .append(endOffset, that.endOffset)
                    .append(position, that.position)
                    .append(positionLength, that.positionLength)
                    .append(hash, that.hash)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(token)
                    .append(startOffset)
                    .append(endOffset)
                    .append(position)
                    .append(positionLength)
                    .append(hash)
                    .toHashCode();
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

        public static class Builder {
            private String token;
            private Long startOffset;
            private Long endOffset;
            private Long position;
            private Long positionLength;
            private Long hash;

            public Builder token(String token) {
                this.token = token;
                return this;
            }

            public Builder startOffset(Long startOffset) {
                this.startOffset = startOffset;
                return this;
            }

            public Builder endOffset(Long endOffset) {
                this.endOffset = endOffset;
                return this;
            }

            public Builder position(Long position) {
                this.position = position;
                return this;
            }

            public Builder positionLength(Long positionLength) {
                this.positionLength = positionLength;
                return this;
            }

            public Builder hash(Long hash) {
                this.hash = hash;
                return this;
            }

            public AnalyzerToken build() {
                return new AnalyzerToken(this);
            }
        }
    }
}
