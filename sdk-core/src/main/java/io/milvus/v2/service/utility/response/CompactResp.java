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

package io.milvus.v2.service.utility.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CompactResp {
    private Long compactionID;

    private CompactResp(Builder builder) {
        this.compactionID = builder.compactionID;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Long getCompactionID() {
        return compactionID;
    }

    public void setCompactionID(Long compactionID) {
        this.compactionID = compactionID;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CompactResp that = (CompactResp) obj;
        return new EqualsBuilder()
                .append(compactionID, that.compactionID)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(compactionID)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CompactResp{" +
                "compactionID=" + compactionID +
                '}';
    }

    public static class Builder {
        private Long compactionID = 0L;

        public Builder compactionID(Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        public CompactResp build() {
            return new CompactResp(this);
        }
    }
}
