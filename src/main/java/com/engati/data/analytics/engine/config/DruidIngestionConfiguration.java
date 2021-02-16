package com.engati.data.analytics.engine.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "engati.data.analytics.engine.druid.ingestion")
@Data
public class DruidIngestionConfiguration {
  private String url;
  private Integer connectTimeout;
  private Integer readTimeout;
}
