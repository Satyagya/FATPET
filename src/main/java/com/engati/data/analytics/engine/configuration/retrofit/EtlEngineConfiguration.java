package com.engati.data.analytics.engine.configuration.retrofit;

import com.engati.data.analytics.engine.constants.constant.Constants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = Constants.ETL_ENGINE_PREFIX)
@Data
public class EtlEngineConfiguration {
    private Integer connectTimeout;
    private Integer readTimeout;
    private String url;
    private String logLevel;
}
