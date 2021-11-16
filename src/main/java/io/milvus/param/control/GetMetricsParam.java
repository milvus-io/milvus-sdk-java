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
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getMetric</code> interface.
 */
@Getter
public class GetMetricsParam {
    private final String request;

    private GetMetricsParam(@NonNull Builder builder) {
        this.request = builder.request;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>GetMetricsParam</code> class.
     */
    public static final class Builder {
        private String request;

        private Builder() {
        }

        /**
         * Set request in json format to retrieve metric information from server.
         * @see <a href="https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy">Metric function design</a>
         *
         * @param request request string in json format
         * @return <code>Builder</code>
         */
        public Builder withRequest(@NonNull String request) {
            this.request = request;
            return this;
        }

        /**
         * Verify parameters and create a new <code>GetMetricsParam</code> instance.
         *
         * @return <code>GetMetricsParam</code>
         */
        public GetMetricsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(request, "Request string");

            // TODO: check the request string is json format

            return new GetMetricsParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>GetMetricsParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetMetricsParam{" +
                "request='" + request + '\'' +
                '}';
    }
}
