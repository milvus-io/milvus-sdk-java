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

package io.milvus.bulkwriter.common.utils;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;

public abstract class ParquetReaderUtils {
    public void readParquet(String localFilePath) throws IOException {
        Path path = new Path(localFilePath);
        try (org.apache.parquet.hadoop.ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(ParquetUtils.getParquetConfiguration())
                .build()) {
            GenericData.Record record;
            while ((record = reader.read()) != null) {
                readRecord(record);
            }
        }
    }

    public abstract void readRecord(GenericData.Record record);
}
