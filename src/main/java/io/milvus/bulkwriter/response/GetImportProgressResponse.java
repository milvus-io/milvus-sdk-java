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
public class GetImportProgressResponse implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;

    private String fileName;

    private Integer fileSize;

    private Double readyPercentage;

    private String completeTime;

    private String errorMessage;

    private String collectionName;

    private String jobId;

    private List<Detail> details;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Detail {
        private String fileName;
        private Integer fileSize;
        private Double readyPercentage;
        private String completeTime;
        private String errorMessage;
    }
}
