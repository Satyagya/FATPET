package com.engati.data.analytics.engine.druid.response;

import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;

import java.util.List;
import java.util.Map;

public interface DruidResponseParser {

  Map<String, List<Map<String, Object>>> convertJsonToMap(JsonArray response,
      Integer botRef, Integer customerId);

  Map<String, List<Map<String, Object>>> convertGroupByJsonToMap(JsonArray response,
      Integer botRef, Integer customerId);

  SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse);
}
