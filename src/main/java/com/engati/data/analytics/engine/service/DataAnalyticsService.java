package com.engati.data.analytics.engine.service;


import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.engati.data.analytics.sdk.response.DruidTaskInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

import java.util.List;

public interface DataAnalyticsService {
  QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request);

  DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId,
      String druidSql);

  DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);

  DataAnalyticsEngineResponse<List<DruidTaskInfo>> getAllTasks();
}
