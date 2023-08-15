package io.milvus.utils;

import lombok.Getter;
import lombok.ToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * @author: wei.hu@zilliz.com
 * @date: 2023/5/1
 */
@Getter
@ToString
public class URLParser {

    private String hostname;
    private int port;
    private String database;
    private boolean secure;

    public URLParser(String url) {
        try {
            // secure
            if (url.startsWith("https://")) {
                secure = true;
            }

            // host
            URI uri = new URI(url);
            hostname = uri.getHost();
            if (Objects.isNull(hostname)) {
                throw new IllegalArgumentException("Missing hostname in url");
            }

            // port
            port = uri.getPort();
            if(port <= 0){
                port = 19530;
            }

            // database
            String path = uri.getPath();
            if (Objects.isNull(path) || path.isEmpty() || "/".equals(path)) {
                database = null;
            } else {
                database = path.substring(1);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url: " + url, e);
        }
    }

}