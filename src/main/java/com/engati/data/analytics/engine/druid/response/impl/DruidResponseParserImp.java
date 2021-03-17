package com.engati.data.analytics.engine.druid.response.impl;

import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class DruidResponseParserImp implements DruidResponseParser {

  public Map<String, List<Map<String, Object>>> convertJsonToMap(JsonArray response,
      Integer botRef, Integer customerId) {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, List<Map<String, Object>>> responseMap = new HashMap<>();
    try {
      for (int index = 0; index < response.size(); index++) {
        String timestamp = response.get(index).getAsJsonObject().get(Constants.TIMESTAMP)
            .getAsString();
        JsonElement jsonElement = response.get(index).getAsJsonObject().get(Constants.RESULT);
        if (jsonElement instanceof JsonArray) {
          List<Map<String, Object>> data = objectMapper
              .readValue(jsonElement.toString(), new TypeReference<List<Map<String, Object>>>() {
              });
          responseMap.put(timestamp, data);
        } else {
          Map<String, Object> data = objectMapper.readValue(jsonElement.toString(), Map.class);
          responseMap.put(timestamp, Collections.singletonList(data));
        }
      }
    } catch (Exception ex) {
      log.error("Exception while parsing the druid response "
          + "from the jsonArray for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return responseMap;
  }

  public Map<String, List<Map<String, Object>>> convertGroupByJsonToMap(JsonArray response, Integer botRef,
      Integer customerId) {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, List<Map<String, Object>>> responseMap = new HashMap<>();
    try {
      for (int index = 0; index < response.size(); index++) {
        String timestamp = response.get(index).getAsJsonObject().get(Constants.TIMESTAMP)
            .getAsString();
        JsonElement jsonElement = response.get(index).getAsJsonObject().get(Constants.EVENT);
        Map<String, Object> data = objectMapper.readValue(jsonElement.toString(), Map.class);
        if (responseMap.containsKey(timestamp)) {
          responseMap.get(timestamp).add(data);
        } else {
          responseMap.put(timestamp, Collections.singletonList(data));
        }
      }
    } catch (Exception ex) {
      log.error("Exception while parsing the druid response "
          + "from the jsonArray for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return responseMap;
  }

  public SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse) {
    if (Objects.isNull(prevResponse) || Objects.isNull(prevResponse.getQueryResponse())
        || prevResponse.getQueryResponse().isEmpty()) {
      return response;
    } else {
      for (String timestamp: response.getQueryResponse().keySet()) {
        if (prevResponse.getQueryResponse().containsKey(timestamp)) {
          prevResponse.getQueryResponse().get(timestamp)
              .addAll(response.getQueryResponse().get(timestamp));
        } else {
          prevResponse.getQueryResponse().put(timestamp, response.getQueryResponse().get(timestamp));
        }
      }
    }
    return prevResponse;
  }
}
