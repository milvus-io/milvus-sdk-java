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

/**
 * Response returned by the {@code getServerVersion} API.
 */
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

    /**
     * Returns the Milvus server version.
     *
     * @return the server version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the Milvus server version.
     *
     * @param version the server version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns the build time of the Milvus server.
     *
     * @return the build time
     */
    public String getBuildTime() {
        return buildTime;
    }

    /**
     * Sets the build time of the Milvus server.
     *
     * @param buildTime the build time
     */
    public void setBuildTime(String buildTime) {
        this.buildTime = buildTime;
    }

    /**
     * Returns the git commit of the Milvus server build.
     *
     * @return the git commit
     */
    public String getGitCommit() {
        return gitCommit;
    }

    /**
     * Sets the git commit of the Milvus server build.
     *
     * @param gitCommit the git commit
     */
    public void setGitCommit(String gitCommit) {
        this.gitCommit = gitCommit;
    }

    /**
     * Returns the Go version used to build the Milvus server.
     *
     * @return the Go version
     */
    public String getGoVersion() {
        return goVersion;
    }

    /**
     * Sets the Go version used to build the Milvus server.
     *
     * @param goVersion the Go version
     */
    public void setGoVersion(String goVersion) {
        this.goVersion = goVersion;
    }

    /**
     * Returns the deployment mode of the Milvus server.
     *
     * @return the deployment mode
     */
    public String getDeployMode() {
        return deployMode;
    }

    /**
     * Sets the deployment mode of the Milvus server.
     *
     * @param deployMode the deployment mode
     */
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

        /**
         * Sets the Milvus server version.
         *
         * @param version the server version
         * @return this builder
         */
        public GetServerVersionRespBuilder version(String version) {
            this.version = version;
            return this;
        }

        /**
         * Sets the build time of the Milvus server.
         *
         * @param buildTime the build time
         * @return this builder
         */
        public GetServerVersionRespBuilder buildTime(String buildTime) {
            this.buildTime = buildTime;
            return this;
        }

        /**
         * Sets the git commit of the Milvus server build.
         *
         * @param gitCommit the git commit
         * @return this builder
         */
        public GetServerVersionRespBuilder gitCommit(String gitCommit) {
            this.gitCommit = gitCommit;
            return this;
        }

        /**
         * Sets the Go version used to build the Milvus server.
         *
         * @param goVersion the Go version
         * @return this builder
         */
        public GetServerVersionRespBuilder goVersion(String goVersion) {
            this.goVersion = goVersion;
            return this;
        }

        /**
         * Sets the deployment mode of the Milvus server.
         *
         * @param deployMode the deployment mode
         * @return this builder
         */
        public GetServerVersionRespBuilder deployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        /**
         * Builds the {@code GetServerVersionResp}.
         *
         * @return the constructed {@code GetServerVersionResp}
         */
        public GetServerVersionResp build() {
            return new GetServerVersionResp(this);
        }
    }
}
