package io.milvus.client.params;

import javax.annotation.Nonnull;
import java.util.List;

public class SearchInFilesParam {
    private final List<String> fileIds;
    private final SearchParam searchParam;

    public static class Builder {
        // Required parameters
        private final List<String> fileIds;
        private final SearchParam searchParam;

        public Builder(List<String> fileIds, SearchParam searchParam) {
            this.fileIds = fileIds;
            this.searchParam = searchParam;
        }

        public SearchInFilesParam build() {
            return new SearchInFilesParam(this);
        }
    }

    private SearchInFilesParam(@Nonnull Builder builder) {
        this.fileIds = builder.fileIds;
        this.searchParam = builder.searchParam;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public SearchParam getSearchParam() {
        return searchParam;
    }
}
