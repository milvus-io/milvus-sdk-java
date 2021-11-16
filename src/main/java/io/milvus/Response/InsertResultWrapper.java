package io.milvus.Response;

import io.milvus.exception.ParamException;
import io.milvus.grpc.MutationResult;

import java.util.List;

import lombok.NonNull;

/**
 * Util class to wrap response of <code>insert</code> interface.
 */
public class InsertResultWrapper {
    private final MutationResult result;

    public InsertResultWrapper(@NonNull MutationResult result) {
        this.result = result;
    }

    /**
     * Get inserted count.
     *
     * @return <code>int</code> inserted count
     */
    public long getInsertCount() {
        return result.getInsertCnt();
    }

    /**
     * Get long id array returned by insert interface.
     * Throw {@link ParamException} if the primary key type is not int64 type.
     *
     * @return <code>List<Long></code> id array returned by insert interface
     */
    public List<Long> getLongIDs() throws ParamException {
        if (result.getIDs().hasIntId()) {
            return result.getIDs().getIntId().getDataList();
        } else {
            throw new ParamException("The primary key is not long type, please try getStringIDs()");
        }
    }

    /**
     * Get string id array returned by insert interface.
     * Throw {@link ParamException} if the primary key type is not string type.
     * Note that currently Milvus doesn't support string type field, this method is reserved.
     *
     * @return <code>List<String></code> id array returned by insert interface
     */
    public List<String> getStringIDs() throws ParamException {
        if (result.getIDs().hasStrId()) {
            return result.getIDs().getStrId().getDataList();
        } else {
            throw new ParamException("The primary key is not string type, please try getLongIDs()");
        }
    }
}
