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

package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

public class SelectGrantForRoleAndObjectParam {

    private final String roleName;

    private final String object;

    private final String objectName;

    private SelectGrantForRoleAndObjectParam(SelectGrantForRoleAndObjectParam.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.roleName = builder.roleName;
        this.object = builder.object;
        this.objectName = builder.objectName;
    }

    public static SelectGrantForRoleAndObjectParam.Builder newBuilder() {
        return new SelectGrantForRoleAndObjectParam.Builder();
    }

    public String getRoleName() {
        return roleName;
    }

    public String getObject() {
        return object;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String toString() {
        return "SelectGrantForRoleAndObjectParam{" +
                "roleName='" + roleName + '\'' +
                ", object='" + object + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }

    /**
     * Builder for {@link SelectGrantForRoleAndObjectParam} class.
     */
    public static final class Builder {
        private String roleName;
        private String object;
        private String objectName;

        private Builder() {
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public SelectGrantForRoleAndObjectParam.Builder withRoleName(String roleName) {
            if (roleName == null) {
                throw new IllegalArgumentException("Role name cannot be null");
            }
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the object. object cannot be empty or null.
         *
         * @param object object
         * @return <code>Builder</code>
         */
        public SelectGrantForRoleAndObjectParam.Builder withObject(String object) {
            if (object == null) {
                throw new IllegalArgumentException("Object cannot be null");
            }
            this.object = object;
            return this;
        }

        /**
         * Sets the objectName. objectName cannot be empty or null.
         *
         * @param objectName objectName
         * @return <code>Builder</code>
         */
        public SelectGrantForRoleAndObjectParam.Builder withObjectName(String objectName) {
            if (objectName == null) {
                throw new IllegalArgumentException("Object name cannot be null");
            }
            this.objectName = objectName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectGrantForRoleAndObjectParam} instance.
         *
         * @return {@link SelectGrantForRoleAndObjectParam}
         */
        public SelectGrantForRoleAndObjectParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");
            ParamUtils.CheckNullEmptyString(object, "Object");
            ParamUtils.CheckNullEmptyString(objectName, "ObjectName");

            return new SelectGrantForRoleAndObjectParam(this);
        }
    }

}
