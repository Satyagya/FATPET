package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.engine.response.ingestion.DruidIngestionResponse;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.google.gson.JsonArray;

public interface DataAnalyticsService {
  QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request);

  DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId, String druidSql);

  DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);
}
