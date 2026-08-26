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

/**
 * The bulk data file types supported by the Milvus BulkWriter, together with their numeric codes and
 * file name suffixes.
 */
public enum BulkFileType {
    /** Parquet bulk file. */
    PARQUET(1, ".parquet"),
    /** JSON bulk file. */
    JSON(2, ".json"),
    /** CSV bulk file. */
    CSV(3, ".csv"),
    ;

    private final Integer code;
    private final String suffix;

    BulkFileType(Integer code, String suffix) {
        this.code = code;
        this.suffix = suffix;
    }

    /**
     * Returns the numeric code of this bulk file type.
     *
     * @return the bulk file type code
     */
    public Integer getCode() {
        return code;
    }

    /**
     * Returns the file name suffix of this bulk file type.
     *
     * @return the file name suffix
     */
    public String getSuffix() {
        return suffix;
    }
}
