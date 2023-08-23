package io.milvus.connection;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.SearchResults;
import io.milvus.param.QueryNodeSingleSearch;
import io.milvus.param.R;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Send heartbeat for query nodes healthy check.
 */
public class QueryNodeListener implements Listener {

    private static final Logger logger = LoggerFactory.getLogger(QueryNodeListener.class);

    private static final int HEARTBEAT_TIMEOUT_MILLS = 4000;

    private final SearchParam searchParam;

    public QueryNodeListener(QueryNodeSingleSearch singleSearch) {
        searchParam = SearchParam.newBuilder()
                .withCollectionName(singleSearch.getCollectionName())
                .withVectors(singleSearch.getVectors())
                .withVectorFieldName(singleSearch.getVectorFieldName())
                .withParams(singleSearch.getParams())
                .withMetricType(singleSearch.getMetricType())
                .withTopK(5)
                .withRoundDecimal(-1)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();
    }

    @Override
    public Boolean heartBeat(ServerSetting serverSetting) {

        boolean isRunning = false;

        try {
            R<SearchResults> response = serverSetting.getClient()
                    .withTimeout(4, TimeUnit.SECONDS)
                    .search(searchParam);

            if (response.getStatus() == R.Status.Success.getCode()) {
                SearchResultsWrapper wrapperSearch = new SearchResultsWrapper(response.getData().getResults());
                List<SearchResultsWrapper.IDScore> idScores = wrapperSearch.getIDScore(0);
                if (CollectionUtils.isNotEmpty(idScores)) {
                    logger.debug("Host [{}] heartbeat Success of Milvus QueryNode Listener.",
                            serverSetting.getServerAddress().getHost());
                    isRunning = true;
                }
            }
        } catch (Exception e) {
            logger.error("Host [{}] heartbeat Error of Milvus QueryNode Listener.",
                    serverSetting.getServerAddress().getHost(), e);
        }
        return isRunning;
    }
}
