package com.engati.data.analytics.engine.service;


import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.engati.data.analytics.sdk.response.QueryResponse;

public interface DataAnalyticsService {

  /**
   * delegates query request to respective handler
   *
   * @param request
   * @param botRef
   * @param customerId
   *
   * @return QueryResponse: result after executing the queries
   */
  QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request);

  /**
   * execute sql queries
   *
   * @param druidSql
   * @param botRef
   * @param customerId
   *
   * @return DataAnalyticsEngineResponse: result after executing the sql query
   */
  DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId,
      String druidSql);

  /**
   * ingest data into druid datasource
   *
   * @param dataSourceName
   * @param botRef
   * @param customerId
   * @param timestamp
   * @param isInitialLoad
   *
   * @return DataAnalyticsEngineResponse: result after executing the sql query
   */
  DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);
}
