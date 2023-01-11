package io.milvus.response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.GetImportStateResponse;
import io.milvus.grpc.ImportState;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.Constant;
import lombok.NonNull;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>GetImportStateResponse</code> interface.
 */
public class GetBulkInsertStateWrapper {
    private final GetImportStateResponse response;

    public GetBulkInsertStateWrapper(@NonNull GetImportStateResponse response) {
        this.response = response;
    }

    /**
     * Gets ID of the bulk insert task.
     *
     * @return Long ID of the bulk insert task
     */
    public long getTaskID() {
        return response.getId();
    }

    /**
     * Gets the long ID array for auto-id primary key, generated by bulk insert task.
     *
     * @return List of Long, ID array returned by bulk insert task
     */
    public List<Long> getAutoGeneratedIDs() {
        // the id list of response is id ranges
        // for example, if the response return [1, 100, 200, 250]
        // the full id list should be [1, 2, 3 ... , 99, 100, 200, 201, 202 ... , 249, 250]
        List<Long> ranges = response.getIdListList();
        if (ranges.size()%2 != 0) {
            throw new IllegalResponseException("The bulk insert state response id range list is illegal");
        }
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < ranges.size()/2; i++) {
            Long begin = ranges.get(i*2);
            Long end = ranges.get(i*2+1);
            for (long j = begin; j <= end; j++) {
                ids.add(j);
            }
        }
        return ids;
    }

    /**
     * Gets state of the bulk insert task.
     *
     * @return ImportState state of the bulk insert task
     */
    public ImportState getState() {
        return response.getState();
    }

    /**
     * Gets how many rows were imported by the bulk insert task.
     *
     * @return Long how many rows were imported by the bulk insert task
     */
    public long getImportedCount() {
        return response.getRowCount();
    }

    /**
     * Gets the integer timestamp when this task is created.
     *
     * @return the integer timestamp when this task is created
     */
    public long getCreateTimestamp() {
        return response.getCreateTs();
    }

    /**
     * Gets the timestamp in string format when this task is created.
     *
     * @return the timestamp in string format when this task is created
     */
    public String getCreateTimeStr() {
        Timestamp ts = new Timestamp(response.getCreateTs());
        return ts.toString();
    }

    /**
     * Gets failed reason of the bulk insert task.
     *
     * @return String failed reason of the bulk insert task
     */
    public String getFailedReason() {
        return getInfo(Constant.FAILED_REASON);
    }

    /**
     * Gets target files of the bulk insert task.
     *
     * @return String target files of the bulk insert task
     */
    public String getFiles() {
        return getInfo(Constant.IMPORT_FILES);
    }

    /**
     * Gets target collection name of the bulk insert task.
     *
     * @return String target collection name
     */
    public String getCollectionName() {
        return getInfo(Constant.IMPORT_COLLECTION);
    }

    /**
     * Gets target partition name of the bulk insert task.
     *
     * @return String target partition name
     */
    public String getPartitionName() {
        return getInfo(Constant.IMPORT_PARTITION);
    }

    /**
     * Gets working progress percent value of the bulk insert task.
     *
     * @return String target collection name
     */
    public int getProgress() {
        String value = getInfo(Constant.IMPORT_PROGRESS);
        if (value.isEmpty()) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    private String getInfo(@NonNull String key) {
        List<KeyValuePair> infos = response.getInfosList();
        for (KeyValuePair kv : infos) {
            if (kv.getKey().compareTo(key) == 0) {
                return kv.getValue();
            }
        }

        return "";
    }

    /**
     * Construct a <code>String</code> by {@link DescCollResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "bulk insert task state{" +
                ", collection:" + getCollectionName() +
                ", partition:" + getPartitionName() +
                ", autoGeneratedIDs:" + getAutoGeneratedIDs() +
                ", state:" + getState().name() +
                ", failed reason:" + getFailedReason() +
                ", files:" + getFiles() +
                ", progress:" + getProgress() +
                '}';
    }
}
