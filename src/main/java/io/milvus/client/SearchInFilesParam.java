package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.List;

public class SearchInFilesParam {
    private final List<String> fileIds;
    private final SearchParam searchParam;
    private final long timeout;

    public static class Builder {
        // Required parameters
        private final List<String> fileIds;
        private final SearchParam searchParam;

        // Optional parameters - initialized to default values
        private long timeout = 86400;

        public Builder(List<String> fileIds, SearchParam searchParam) {
            this.fileIds = fileIds;
            this.searchParam = searchParam;
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public SearchInFilesParam build() {
            return new SearchInFilesParam(this);
        }
    }

    private SearchInFilesParam(@Nonnull Builder builder) {
        this.fileIds = builder.fileIds;
        this.searchParam = builder.searchParam;
        this.timeout = builder.timeout;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public SearchParam getSearchParam() {
        return searchParam;
    }

    public long getTimeout() {
        return timeout;
    }
}
