package com.engati.data.analytics.engine.druid.response.impl;

import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
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

  public List<List<Map<String, String>>> convertJsonToMap(JsonArray response) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<List<Map<String, String>>> responseMapList = new ArrayList<>();
    try {
      for (int i = 0; i < response.size(); i++) {
        JsonElement jsonElement = response.get(i).getAsJsonObject().get("result");
        if (jsonElement instanceof JsonArray) {
          List<Map<String, String>> data = objectMapper
              .readValue(jsonElement.toString(), new TypeReference<List<Map<String, String>>>() {
              });
          responseMapList.add(data);
        } else {
          Map<String, String> data = objectMapper.readValue(jsonElement.toString(), Map.class);
          responseMapList.add(Collections.singletonList(data));
        }
      }
    } catch (Exception e) {
      log.error("Error while generating response from the jsonArray", e);
    }
    return responseMapList;
  }

  public List<Map<String, Object>> convertGroupByJsonToMap(JsonArray response) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<Map<String, Object>> responseMapList = new ArrayList<>();
    try {
      for (int i = 0; i < response.size(); i++) {
        JsonElement jsonElement = response.get(i).getAsJsonObject().get("event");
        Map<String, Object> data = objectMapper.readValue(jsonElement.toString(), Map.class);
        responseMapList.add(data);
      }
    } catch (Exception e) {
      log.error("Error while generating response from the jsonArray", e);
    }
    return responseMapList;
  }

}
