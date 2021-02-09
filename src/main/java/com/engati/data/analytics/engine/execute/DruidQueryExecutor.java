package com.engati.data.analytics.engine.execute;

import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.google.gson.JsonArray;

public interface DruidQueryExecutor {

  JsonArray getResponseFromDruid(String druidJsonQuery);
  DataAnalyticsEngineResponse<String> getDruidSqlResponse(Long customerId, Long botRef, String druidSqlQuery);
}
