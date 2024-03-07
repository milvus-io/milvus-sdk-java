package io.milvus.bulkwriter.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ListImportJobsResponse implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;

    private Integer count;

    private Integer currentPage;

    private Integer pageSize;

    private List<Record> records;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Record {
        private String collectionName;
        private String jobId;
        private String state;
    }
}
