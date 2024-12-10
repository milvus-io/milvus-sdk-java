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

package io.milvus.param.control;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>getCompactionStateWithPlans</code> interface.
 *
 * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+16+--+Compaction">Compaction function design</a>
 */
@Getter
@ToString
public class GetCompactionPlansParam {
    private final Long compactionID;

    private GetCompactionPlansParam(@NonNull Builder builder) {
        this.compactionID = builder.compactionID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetCompactionPlansParam} class.
     */
    public static final class Builder {
        private Long compactionID;

        private Builder() {
        }

        /**
         * Sets compaction action id to get the plans.
         *
         * @param compactionID compaction action id
         * @return <code>Builder</code>
         */
        public Builder withCompactionID(@NonNull Long compactionID) {
            this.compactionID = compactionID;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetCompactionPlansParam} instance.
         *
         * @return {@link GetCompactionPlansParam}
         */
        public GetCompactionPlansParam build() throws ParamException {
            return new GetCompactionPlansParam(this);
        }
    }
}
