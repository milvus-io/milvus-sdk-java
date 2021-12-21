package io.milvus.Response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to wrap response of <code>search</code> interface.
 */
public class SearchResultsWrapper {
    private final SearchResultData results;

    public SearchResultsWrapper(@NonNull SearchResultData results) {
        this.results = results;
    }

    /**
     * Gets data for an output field which is specified by search request.
     * Throws {@link ParamException} if the field doesn't exist.
     * Throws {@link ParamException} if the indexOfTarget is illegal.
     *
     * @param fieldName field name to get output data
     * @param indexOfTarget which target vector the field data belongs to
     * @return <code>FieldDataWrapper</code>
     */
    public List<?> getFieldData(@NonNull String fieldName, int indexOfTarget) {
        FieldDataWrapper wrapper = null;
        for (int i = 0; i < results.getFieldsDataCount(); ++i) {
            FieldData data = results.getFieldsData(i);
            if (fieldName.compareTo(data.getFieldName()) == 0) {
                wrapper = new FieldDataWrapper(data);
            }
        }

        if (wrapper == null) {
            throw new ParamException("Illegal field name: " + fieldName);
        }

        Position position = getOffsetByIndex(indexOfTarget);
        long offset = position.getOffset();
        long k = position.getK();

        List<?> allData = wrapper.getFieldData();
        if (offset + k > allData.size()) {
            throw new IllegalResponseException("Field data row count is wrong");
        }

        return allData.subList((int)offset, (int)offset + (int)k);
    }

    /**
     * Gets ID-score pairs returned by search interface.
     * Throws {@link ParamException} if the indexOfTarget is illegal.
     * Throws {@link IllegalResponseException} if the returned results is illegal.
     *
     * @param indexOfTarget which target vector the result belongs to
     * @return <code>List<IDScore></code> ID-score pairs returned by search interface
     */
    public List<IDScore> getIDScore(int indexOfTarget) throws ParamException, IllegalResponseException {
        Position position = getOffsetByIndex(indexOfTarget);

        long offset = position.getOffset();
        long k = position.getK();
        if (offset + k > results.getScoresCount()) {
            throw new IllegalResponseException("Result scores count is wrong");
        }

        List<IDScore> idScore = new ArrayList<>();

        IDs ids = results.getIds();
        if (ids.hasIntId()) {
            LongArray longIDs = ids.getIntId();
            if (offset + k > longIDs.getDataCount()) {
                throw new IllegalResponseException("Result ids count is wrong");
            }

            for (int n = 0; n < k; ++n) {
                idScore.add(new IDScore("", longIDs.getData((int)offset + n), results.getScores((int)offset + n)));
            }
        } else if (ids.hasStrId()) {
            StringArray strIDs = ids.getStrId();
            if (offset + k >= strIDs.getDataCount()) {
                throw new IllegalResponseException("Result ids count is wrong");
            }

            for (int n = 0; n < k; ++n) {
                idScore.add(new IDScore(strIDs.getData((int)offset + n), 0, results.getScores((int)offset + n)));
            }
        } else {
            throw new IllegalResponseException("Result ids is illegal");
        }

        return idScore;
    }

    @Getter
    private static final class Position {
        private final long offset;
        private final long k;

        public Position(long offset, long k) {
            this.offset = offset;
            this.k = k;
        }
    }
    private Position getOffsetByIndex(int indexOfTarget) {
        List<Long> kList = results.getTopksList();

        // if the server didn't return separate topK, use same topK value
        if (kList.isEmpty()) {
            kList = new ArrayList<>();
            for (long i = 0; i < results.getNumQueries(); ++i) {
                kList.add(results.getTopK());
            }
        }

        if (indexOfTarget < 0 || indexOfTarget >= kList.size()) {
            throw new ParamException("Illegal index of target: " + indexOfTarget);
        }

        long offset = 0;
        for (int i = 0; i < indexOfTarget; ++i) {
            offset += kList.get(i);
        }

        long k = kList.get(indexOfTarget);
        return new Position(offset, k);
    }

    /**
     * Internal-use class to wrap response of <code>search</code> interface.
     */
    @Getter
    public static final class IDScore {
        private final String strID;
        private final long longID;
        private final float score;

        public IDScore(String strID, long longID, float score) {
            this.strID = strID;
            this.longID = longID;
            this.score = score;
        }

        @Override
        public String toString() {
            if (strID.isEmpty()) {
                return "(ID: " + getLongID() + " Score: " + getScore() + ")";
            } else {
                return "(ID: '" + getStrID() + "' Score: " + getScore() + ")";
            }
        }
    }
}
