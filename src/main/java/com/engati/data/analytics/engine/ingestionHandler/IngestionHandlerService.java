package com.engati.data.analytics.engine.ingestionHandler;

import com.engati.data.analytics.engine.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.engine.response.ingestion.DruidIngestionResponse;

public interface IngestionHandlerService {

  DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);
}
