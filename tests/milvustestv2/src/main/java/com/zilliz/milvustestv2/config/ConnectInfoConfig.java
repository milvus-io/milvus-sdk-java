package com.zilliz.milvustestv2.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Author yongpeng.li
 * @Date 2024/2/23 10:41
 */

@Data
@Component
@ConfigurationProperties(prefix = "connectinfo")
public class ConnectInfoConfig {
    String uri;
}
