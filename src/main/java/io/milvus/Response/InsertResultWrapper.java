package io.milvus.Response;

import io.milvus.exception.ParamException;
import io.milvus.grpc.MutationResult;

import java.util.List;

import lombok.NonNull;

public class InsertResultWrapper {
    private final MutationResult result;

    public InsertResultWrapper(@NonNull MutationResult result) {
        this.result = result;
    }

    public long getInsertCount() {
        return result.getInsertCnt();
    }

    public List<Long> getLongIDs() throws ParamException {
        if (result.getIDs().hasIntId()) {
            return result.getIDs().getIntId().getDataList();
        } else {
            throw new ParamException("The primary key is not long type, please try getStringIDs()");
        }
    }

    public List<String> getStringIDs() throws ParamException {
        if (result.getIDs().hasStrId()) {
            return result.getIDs().getStrId().getDataList();
        } else {
            throw new ParamException("The primary key is not string type, please try getLongIDs()");
        }
    }
}
