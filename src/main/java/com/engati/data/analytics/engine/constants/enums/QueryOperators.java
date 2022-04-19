package com.engati.data.analytics.engine.constants.enums;

import lombok.Getter;
import lombok.ToString;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
public enum QueryOperators {
  LESS_THAN("LT","<"),
  LESS_THAN_EQUAL("LTE", "<="),
  GREATER_THAN("GT", ">"),
  GREATER_THAN_EQUAL("GTE", ">="),
  EQUAL("EQUAL","=");

  public final String label;
  public final String notation;

  QueryOperators(String label, String notation) {
    this.label = label;
    this.notation = notation;
  }

  private static final Map<String, String> OPERATOR_STRING_MAP =
      Arrays.stream(values()).collect(Collectors.toMap(QueryOperators::getLabel, QueryOperators::getNotation));

  public static String getOperator(String Operator) {
    return OPERATOR_STRING_MAP.get(Operator);
  }


}
