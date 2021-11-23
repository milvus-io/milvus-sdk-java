package io.milvus.Response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>search</code> interface.
 */
public class SearchResultsWrapper {
    private final SearchResultData results;

    public SearchResultsWrapper(@NonNull SearchResultData results) {
        this.results = results;
    }

    /**
     * Get {@link FieldDataWrapper} for a field.
     * Throws {@link ParamException} if the field doesn't exist.
     *
     * @return <code>FieldDataWrapper</code>
     */
    public FieldDataWrapper GetFieldData(@NonNull String fieldName) {
        for (int i = 0; i < results.getFieldsDataCount(); ++i) {
            FieldData data = results.getFieldsData(i);
            if (fieldName.compareTo(data.getFieldName()) == 0) {
                return new FieldDataWrapper(data);
            }
        }

        return null;
    }

    /**
     * Get id-score pairs returned by search interface.
     * Throw {@link ParamException} if the indexOfTarget is illegal.
     * Throw {@link IllegalResponseException} if the returned results is illegal.
     *
     * @return <code>List<IDScore></code> id-score pairs returned by search interface
     */
    public List<IDScore> GetIDScore(int indexOfTarget) throws ParamException, IllegalResponseException {
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

        int offset = 0;
        for (int i = 0; i < indexOfTarget; ++i) {
            offset += kList.get(i);
        }

        long k = kList.get(indexOfTarget);
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
                idScore.add(new IDScore("", longIDs.getData(offset + n), results.getScores(offset + n)));
            }
        } else if (ids.hasStrId()) {
            StringArray strIDs = ids.getStrId();
            if (offset + k >= strIDs.getDataCount()) {
                throw new IllegalResponseException("Result ids count is wrong");
            }

            for (int n = 0; n < k; ++n) {
                idScore.add(new IDScore(strIDs.getData(offset + n), 0, results.getScores(offset + n)));
            }
        } else {
            throw new IllegalResponseException("Result ids is illegal");
        }

        return idScore;
    }

    /**
     * Internal use class to wrap response of <code>search</code> interface.
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
                return "(ID: " + longID + " Score: " + score + ")";
            } else {
                return "(ID: '" + strID + "' Score: " + score + ")";
            }
        }
    }
}
