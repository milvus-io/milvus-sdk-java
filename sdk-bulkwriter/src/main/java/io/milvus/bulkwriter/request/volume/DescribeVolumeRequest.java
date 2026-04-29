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

package io.milvus.bulkwriter.request.volume;

public class DescribeVolumeRequest {
    private String volumeName;

    public DescribeVolumeRequest() {
    }

    public DescribeVolumeRequest(String volumeName) {
        this.volumeName = volumeName;
    }

    protected DescribeVolumeRequest(DescribeVolumeRequestBuilder builder) {
        this.volumeName = builder.volumeName;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    @Override
    public String toString() {
        return "DescribeVolumeRequest{" +
                "volumeName='" + volumeName + '\'' +
                '}';
    }

    public static DescribeVolumeRequestBuilder builder() {
        return new DescribeVolumeRequestBuilder();
    }

    public static class DescribeVolumeRequestBuilder {
        private String volumeName;

        private DescribeVolumeRequestBuilder() {
            this.volumeName = "";
        }

        public DescribeVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public DescribeVolumeRequest build() {
            return new DescribeVolumeRequest(this);
        }
    }
}
