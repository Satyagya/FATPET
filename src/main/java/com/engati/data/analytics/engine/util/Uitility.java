package com.engati.data.analytics.engine.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.query.DruidQuery;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Uitility {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static String convertDruidQueryToJsonString(DruidQuery druidQuery) {
    String requiredJson = null;
    try {
      requiredJson = MAPPER.writeValueAsString(druidQuery);
    } catch (JsonProcessingException ex) {
      log.error("jsonParsingException", ex);
    }
    return requiredJson;
  }

  public static String convertDataSource(Integer botRef, Integer customerId, String dataSource) {
    return String.format("%s_%s_%s", dataSource, botRef, customerId);
  }
}
