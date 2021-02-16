package com.engati.data.analytics.engine.druid.response.impl;

import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DruidResponseParserImp implements DruidResponseParser {

  public List<List<Map<String, Object>>> convertJsonToMap(JsonArray response, Integer botRef,
      Integer customerId) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<List<Map<String, Object>>> responseMapList = new ArrayList<>();
    try {
      for (int index = 0; index < response.size(); index++) {
        JsonElement jsonElement = response.get(index).getAsJsonObject().get(Constants.RESULT);
        if (jsonElement instanceof JsonArray) {
          List<Map<String, Object>> data = objectMapper
              .readValue(jsonElement.toString(), new TypeReference<List<Map<String, Object>>>() {
              });
          responseMapList.add(data);
        } else {
          Map<String, Object> data = objectMapper.readValue(jsonElement.toString(), Map.class);
          responseMapList.add(Collections.singletonList(data));
        }
      }
    } catch (Exception ex) {
      log.error("DruidResponseParserImp: Error while parsing the druid response "
          + "from the jsonArray for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return responseMapList;
  }

  public List<Map<String, Object>> convertGroupByJsonToMap(JsonArray response, Integer botRef,
      Integer customerId) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<Map<String, Object>> responseMapList = new ArrayList<>();
    try {
      for (int index = 0; index < response.size(); index++) {
        JsonElement jsonElement = response.get(index).getAsJsonObject().get(Constants.EVENT);
        Map<String, Object> data = objectMapper.readValue(jsonElement.toString(), Map.class);
        responseMapList.add(data);
      }
    } catch (Exception ex) {
      log.error("DruidResponseParserImp: Error while parsing the druid response "
          + "from the jsonArray for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return responseMapList;
  }

}
