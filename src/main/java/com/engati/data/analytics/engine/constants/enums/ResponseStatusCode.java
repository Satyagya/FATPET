package com.engati.data.analytics.engine.constants.enums;

import com.nethum.errorhandling.exception.error.AppCode;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ResponseStatusCode implements AppCode<ResponseStatusCode> {
  PROCESSING_ERROR(999, "PROCESSING_ERROR"),
  SUCCESS(1000, "SUCCESS"),
  EMPTY_SEGMENT(9999, "EMPTY_SEGMENT"),
  DUCK_DB_QUERY_FAILURE(9998, "DUCK_DB_QUERY_FAILURE"),
  CSV_CREATION_EXCEPTION(9997,  "CSV_CREATION_EXCEPTION"),
  OPERATORS_PERMISSIBLE_LIMITS_REACHED(9996, "OPERATORS_PERMISSIBLE_LIMITS_REACHED"),
  INVALID_ATTRIBUTES_PROVIDED(9995, "INVALID_ATTRIBUTES_PROVIDED"),
  INVALID_FILE_TYPE(9994, "INVALID_FILE_TYPE"),
  INVALID_AUTH_JSON_FILE(9993, "INVALID_AUTH_JSON_FILE");

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
