package io.milvus.response;

import io.milvus.exception.ParamException;
import io.milvus.grpc.MutationResult;

import java.util.List;

import lombok.NonNull;

/**
 * Utility class to wrap response of <code>insert/delete</code> interface.
 */
public class MutationResultWrapper {
    private final MutationResult result;

    public MutationResultWrapper(@NonNull MutationResult result) {
        this.result = result;
    }

    /**
     * Gets the row count of the inserted entities.
     *
     * @return <code>int</code> row count of the inserted entities
     */
    public long getInsertCount() {
        return result.getInsertCnt();
    }

    /**
     * Gets the long ID array returned by insert interface.
     * Throw {@link ParamException} if the primary key type is not int64 type.
     *
     * @return List&lt;Long&gt; ID array returned by insert interface
     */
    public List<Long> getLongIDs() throws ParamException {
        if (result.getIDs().hasIntId()) {
            return result.getIDs().getIntId().getDataList();
        } else {
            throw new ParamException("The primary key is not long type, please try getStringIDs()");
        }
    }

    /**
     * Gets the string ID array returned by insert interface.
     * Throw {@link ParamException} if the primary key type is not string type.
     * Note that current release of Milvus doesn't support string type field, thus this method is reserved.
     *
     * @return List&lt;String&gt; ID array returned by insert interface
     */
    public List<String> getStringIDs() throws ParamException {
        if (result.getIDs().hasStrId()) {
            return result.getIDs().getStrId().getDataList();
        } else {
            throw new ParamException("The primary key is not string type, please try getLongIDs()");
        }
    }

    /**
     * Gets the row count of the deleted entities. Currently this value is always equal to input row count
     *
     * @return <code>int</code> row count of the deleted entities
     */
    public long getDeleteCount() {
        return result.getDeleteCnt();
    }

    /**
     * Get timestamp of the operation marked by server. You can use this timestamp as for guarantee timestamp of query/search api.
     *
     * Note: the timestamp is not an absolute timestamp, it is a hybrid value combined by UTC time and internal flags.
     *  We call it TSO, for more information: @see <a href="https://github.com/milvus-io/milvus/blob/master/docs/design_docs/milvus_hybrid_ts_en.md">Hybrid Timestamp in Milvus</a>
     *
     * @return <code>int</code> row count of the deleted entities
     */
    public long getOperationTs() {
        return result.getTimestamp();
    }
}
