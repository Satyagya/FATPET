package com.engati.data.analytics.engine.druid.response;

import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;

import java.util.List;
import java.util.Map;

public interface DruidResponseParser {

  /**
   * parsing druid native query response from time-series and top-N
   *
   * @param customerId: customer identifier
   * @param botRef: bot reference
   * @param response: raw response from druid
   *
   * @return Map: (k, v) -> (timestamp, calculated response)
   */
  Map<String, List<Map<String, Object>>> convertJsonToMap(JsonArray response,
      Integer botRef, Integer customerId);

  /**
   * parsing druid native query response from groupBy queries
   *
   * @param customerId: customer identifier
   * @param botRef: bot reference
   * @param response: raw response from druid
   *
   * @return Map: (k, v) -> (timestamp, calculated response)
   */
  Map<String, List<Map<String, Object>>> convertGroupByJsonToMap(JsonArray response,
      Integer botRef, Integer customerId);

  /**
   * merging two time-series response
   *
   * @param response
   * @param prevResponse
   *
   * @return SimpleResponse
   */
  SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse);
}
