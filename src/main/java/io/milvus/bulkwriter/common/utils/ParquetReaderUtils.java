package io.milvus.bulkwriter.common.utils;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;

public abstract class ParquetReaderUtils {
    public void readParquet(String localFilePath) throws IOException {
        Path path = new Path(localFilePath);
        try (org.apache.parquet.hadoop.ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(new Configuration())
                .build()) {
            GenericData.Record record;
            while ((record = reader.read()) != null) {
                readRecord(record);
            }
        }
    }

    public abstract void readRecord(GenericData.Record record);
}
