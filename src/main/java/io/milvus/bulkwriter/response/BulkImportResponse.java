package io.milvus.bulkwriter.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BulkImportResponse implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;

    private String jobId;
}
