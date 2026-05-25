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

public class GetServerVersionResp {
    private String version;
    private String buildTime;
    private String gitCommit;
    private String goVersion;
    private String deployMode;

    private GetServerVersionResp(GetServerVersionRespBuilder builder) {
        this.version = builder.version;
        this.buildTime = builder.buildTime;
        this.gitCommit = builder.gitCommit;
        this.goVersion = builder.goVersion;
        this.deployMode = builder.deployMode;
    }

    public static GetServerVersionRespBuilder builder() {
        return new GetServerVersionRespBuilder();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBuildTime() {
        return buildTime;
    }

    public void setBuildTime(String buildTime) {
        this.buildTime = buildTime;
    }

    public String getGitCommit() {
        return gitCommit;
    }

    public void setGitCommit(String gitCommit) {
        this.gitCommit = gitCommit;
    }

    public String getGoVersion() {
        return goVersion;
    }

    public void setGoVersion(String goVersion) {
        this.goVersion = goVersion;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    @Override
    public String toString() {
        return "GetServerVersionResp{" +
                "version='" + version + '\'' +
                ", buildTime='" + buildTime + '\'' +
                ", gitCommit='" + gitCommit + '\'' +
                ", goVersion='" + goVersion + '\'' +
                ", deployMode='" + deployMode + '\'' +
                '}';
    }

    public static class GetServerVersionRespBuilder {
        private String version;
        private String buildTime;
        private String gitCommit;
        private String goVersion;
        private String deployMode;

        public GetServerVersionRespBuilder version(String version) {
            this.version = version;
            return this;
        }

        public GetServerVersionRespBuilder buildTime(String buildTime) {
            this.buildTime = buildTime;
            return this;
        }

        public GetServerVersionRespBuilder gitCommit(String gitCommit) {
            this.gitCommit = gitCommit;
            return this;
        }

        public GetServerVersionRespBuilder goVersion(String goVersion) {
            this.goVersion = goVersion;
            return this;
        }

        public GetServerVersionRespBuilder deployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        public GetServerVersionResp build() {
            return new GetServerVersionResp(this);
        }
    }
}
