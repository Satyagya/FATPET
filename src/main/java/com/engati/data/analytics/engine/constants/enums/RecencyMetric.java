package com.engati.data.analytics.engine.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
@ToString
public enum RecencyMetric {
  LAST_ORDER_DATE("LAST_ORDER_DATE"),
  FIRST_ORDER_DATE("FIRST_ORDER_DATE");

  private final String name;

  private static final Map<String, String> RECENCY_METRIC_STRING_MAP =
      Arrays.stream(values()).collect(Collectors.toMap(Enum::name, RecencyMetric::getName));

  public static String fromRecencyMetric(String name) {
    return RECENCY_METRIC_STRING_MAP.get(name);
  }


}

