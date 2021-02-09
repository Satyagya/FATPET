package com.engati.data.analytics.engine.druid.response;

import com.google.gson.JsonArray;

import java.util.List;
import java.util.Map;

public interface DruidResponseParser {

  List<List<Map<String, Object>>> convertJsonToMap(JsonArray response);

  List<Map<String, Object>> convertGroupByJsonToMap(JsonArray response);
}
