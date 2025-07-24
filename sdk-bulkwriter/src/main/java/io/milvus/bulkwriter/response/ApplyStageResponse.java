package io.milvus.bulkwriter.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;


@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class ApplyStageResponse implements Serializable {
    private String endpoint;

    private String cloud;

    private String region;

    private String bucketName;

    private String uploadPath;

    private Credentials credentials;

    private Condition condition;

    private String stageName;

    private String stagePrefix;

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class Credentials implements Serializable {
        private static final long serialVersionUID = 623702599895113789L;
        private String tmpAK;
        private String tmpSK;
        private String sessionToken;
        private String expireTime;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class Condition implements Serializable {
        private static final long serialVersionUID = -2613029991242322109L;
        private Long maxContentLength;
    }
}
