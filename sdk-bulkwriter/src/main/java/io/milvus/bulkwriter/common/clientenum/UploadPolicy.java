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

package io.milvus.bulkwriter.common.clientenum;

/** Decides whether an existing object at the exact target key can be skipped. */
public enum UploadPolicy {
    /** Upload every source file without listing the destination. */
    OVERWRITE,
    /** Skip any existing target key, regardless of its content or metadata. */
    SKIP_IF_EXISTS,
    /** Skip an existing target key when its size equals the local file size. The default. */
    SKIP_IF_SAME_SIZE,
    /**
     * Skip when sizes match and the local modification time is not later than the object's
     * server LastModified time. This is an incremental-update heuristic, not content equality:
     * clock skew and files with preserved/backdated timestamps can hide changes.
     */
    SIZE_AND_MTIME;

    /**
     * Called only after an exact target-key match. Missing required metadata means upload.
     * Times are milliseconds since the epoch; no clock-skew tolerance is applied.
     */
    public boolean shouldSkip(long localSize, long localModifiedTimeMillis,
                              Long remoteSize, Long remoteModifiedTimeMillis) {
        if (this == OVERWRITE) { return false; }
        if (this == SKIP_IF_EXISTS) { return true; }
        if (remoteSize == null || remoteSize < 0 || remoteSize != localSize) { return false; }
        return this == SKIP_IF_SAME_SIZE
                || (remoteModifiedTimeMillis != null && localModifiedTimeMillis <= remoteModifiedTimeMillis);
    }
}
