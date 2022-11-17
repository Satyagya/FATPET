package com.engati.data.analytics.engine.constants.enums;

import com.nethum.errorhandling.exception.error.AppCode;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ResponseStatusCode implements AppCode<ResponseStatusCode> {
  SUCCESS(1000, "SUCCESS"),
  PROCESSING_ERROR(999, "PROCESSING_ERROR"),
  EMPTY_SEGMENT(9999, "EMPTY_SEGMENT"),
  DUCK_DB_QUERY_FAILURE(9998, "DUCK_DB_QUERY_FAILURE"),
  CSV_CREATION_EXCEPTION(9997,  "CSV_CREATION_EXCEPTION"),
  OPERATORS_PERMISSIBLE_LIMITS_REACHED(9996, "OPERATORS_PERMISSIBLE_LIMITS_REACHED"),
  INVALID_ATTRIBUTES_PROVIDED(9995, "INVALID_ATTRIBUTES_PROVIDED"),
  INVALID_FILE_TYPE_OR_PROPERTY_ID(9994, "INVALID_FILE_TYPE_OR_PROPERTY_ID"),
  INVALID_AUTH_JSON_FILE(9993, "INVALID_AUTH_JSON_FILE"),
  INPUT_MISSING(9992, "INPUT_PARAMETERS_MISSING"),
  NO_PRODUCTS_FOUND(9991, "NO_PRODUCTS_FOUND"),
  INVALID_EXPRESSION_CONDITION_PROVIDED(9990,"INVALID_EXPRESSION_CONDITION_PROVIDED"),
  INVALID_TOTAL_NUMBER_OF_CONDITIONS(9989,"INVALID_TOTAL_NUMBER_OF_CONDITIONS"),
  START_DATE_IS_NULL(9988,"START_DATE_IS_NULL"),
  END_DATE_IS_NULL(9987,"END_DATE_IS_NULL"),
  DATE_RANGE_IS_NOT_VALID(8888,"DATE_RANGE_IS_NOT_VALID"),
  ;

  private final int code;
  private final String desc;

  private static Map<Integer, ResponseStatusCode> FORMAT_MAP =
      Stream.of(ResponseStatusCode.values())
          .collect(Collectors.toMap(s -> s.code, Function.identity()));

  ResponseStatusCode(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  @Override
  public ResponseStatusCode valueOf(int i) {
    for (ResponseStatusCode status : values()) {
      if (status.code == i) {
        return status;
      }
    }
    throw new IllegalArgumentException("No matching status found for code " + i);
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public String getDesc() {
    return desc;
  }

}
