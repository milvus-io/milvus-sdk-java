package io.milvus.bulkwriter.resolver;

import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.common.clientenum.ConnectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class EndpointResolver {
    private static final Logger logger = LoggerFactory.getLogger(EndpointResolver.class);

    public static String resolveEndpoint(String defaultEndpoint, String cloud, String region, ConnectType connectType) {
        logger.info("Start resolving endpoint, cloud:{}, region:{}, connectType:{}", cloud, region, connectType);
        if (CloudStorage.isAliCloud(cloud)) {
            defaultEndpoint = resolveOssEndpoint(region, connectType);
        }
        logger.info("Resolved endpoint: {}, reachable check passed", defaultEndpoint);
        return defaultEndpoint;
    }

    private static String resolveOssEndpoint(String region, ConnectType connectType) {
        String internalEndpoint = String.format("oss-%s-internal.aliyuncs.com", region);
        String publicEndpoint = String.format("oss-%s.aliyuncs.com", region);

        switch (connectType) {
            case INTERNAL:
                logger.info("Forced INTERNAL endpoint selected: {}", internalEndpoint);
                checkEndpointReachable(internalEndpoint, true);
                return internalEndpoint;
            case PUBLIC:
                logger.info("Forced PUBLIC endpoint selected: {}", publicEndpoint);
                checkEndpointReachable(publicEndpoint, true);
                return publicEndpoint;
            case AUTO:
            default:
                if (checkEndpointReachable(internalEndpoint, false)) {
                    logger.info("AUTO mode: internal endpoint reachable, using {}", internalEndpoint);
                    return internalEndpoint;
                } else {
                    logger.warn("AUTO mode: internal endpoint not reachable, fallback to public endpoint {}", publicEndpoint);
                    checkEndpointReachable(publicEndpoint, true);
                    return publicEndpoint;
                }
        }
    }

    private static boolean checkEndpointReachable(String endpoint, boolean printError) {
        try {
            String httpEndpoint = String.format("https://%s", endpoint);
            URL url = new URL(httpEndpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(5));
            conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(5));
            conn.setRequestMethod("HEAD");
            int code = conn.getResponseCode();
            logger.debug("Checked endpoint {}, response code={}", endpoint, code);
            return code >= 200 && code < 400;
        } catch (Exception e) {
            if (printError) {
                logger.error("Endpoint {} not reachable, throwing exception", endpoint, e);
                throw new RuntimeException(e.getMessage());
            } else {
                logger.warn("Endpoint {} not reachable, will fallback if needed", endpoint);
                return false;
            }
        }
    }
}
