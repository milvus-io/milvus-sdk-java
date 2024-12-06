package io.milvus.bulkwriter.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Record {
    private String collectionName;
    private String jobId;
    private String state;
}
