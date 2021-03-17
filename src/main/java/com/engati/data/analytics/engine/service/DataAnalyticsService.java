package com.engati.data.analytics.engine.service;


import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.engati.data.analytics.sdk.response.DruidTaskInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

import java.util.List;

public interface DataAnalyticsService {
  /**
   * gets the response for provided query request
   *
   * @param botRef
   * @param customerId
   * @param request
   *
   * @return
   */
  QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request);

  /**
   * gets response for druid sql queries
   *
   * @param botRef
   * @param customerId
   * @param druidSql
   *
   * @return
   */
  DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId,
      String druidSql);

  /**
   * triggers ingestion to druid
   *
   * @param customerId
   * @param botRef
   * @param timestamp
   * @param dataSourceName
   * @param isInitialLoad
   *
   * @return
   */
  DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);

  /**
   * gets list of all ingestion tasks
   *
   * @return
   */
  DataAnalyticsEngineResponse<List<DruidTaskInfo>> ingestionTaskListResponse();
}
