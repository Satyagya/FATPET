package com.engati.data.analytics.engine.execute;

import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.google.gson.JsonArray;

public interface DruidQueryExecutor {

  /**
   * execute druid sql query
   *
   * @param customerId
   * @param botRef
   * @param druidSqlQuery
   *
   * @return DataAnalyticsEngineResponse
   */
  DataAnalyticsEngineResponse<String> getDruidSqlResponse(Long customerId, Long botRef,
      String druidSqlQuery);

  /**
   * execute druid native query
   *
   * @param customerId
   * @param botRef
   * @param druidJsonQuery
   *
   * @return DataAnalyticsEngineResponse
   */
  JsonArray getResponseFromDruid(String druidJsonQuery, Integer botRef, Integer customerId);
}
