package io.milvus.connection;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Send heartbeat for a Milvus cluster healthy check.
 */
public class ClusterListener implements Listener {

    private static final Logger logger = LoggerFactory.getLogger(ClusterListener.class);

    private static final String HEALTH_PATH = "http://%s:%d/healthz";

    private static final int HTTP_CODE_200 = 200;
    private static final String RESPONSE_OK = "OK";

    private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .build();

    @Override
    public Boolean heartBeat(ServerSetting serverSetting) {
        String url = String.format(HEALTH_PATH, serverSetting.getServerAddress().getHost(),
                serverSetting.getServerAddress().getHealthPort());

        boolean isRunning = false;
        try {
            Response response = get(url);
            isRunning = checkResponse(response);
            if (isRunning) {
                logger.debug("Host [{}] heartbeat Success of Milvus Cluster Listener.",
                        serverSetting.getServerAddress().getHost());
            }
        } catch (Exception e) {
            logger.error("Host [{}] heartbeat Error of Milvus Cluster Listener.",
                    serverSetting.getServerAddress().getHost());
        }
        return isRunning;
    }

    private Boolean checkResponse(Response response) throws IOException {
        if (HTTP_CODE_200 == response.code()) {
            assert response.body() != null;
            String responseBody = response.body().string();
            return RESPONSE_OK.equalsIgnoreCase(responseBody);
        }
        return false;
    }

    private Response get(String url) throws IOException {
        if (StringUtils.isEmpty(url)) {
            throw new IllegalArgumentException("OkHttp GET error: url cannot be null.");
        }

        Request.Builder requestBuilder = new Request.Builder();

        Request request = requestBuilder
                .url(url)
                .get()
                .build();
        return OK_HTTP_CLIENT.newCall(request).execute();
    }
}
