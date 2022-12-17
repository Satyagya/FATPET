package com.personal.fatpet.model;

public enum FatPetStatusCode {
  SUCCESS(200, "SUCCESS"),
  FAIL(400,"FAIL"),
  DATA_VALIDATION_FAILED(403,"DATA_VALIDATION_FAILED"),
  PROCESSING_ERROR(999,"PROCESSING_ERROR");

  private final int code;
  private final String desc;

  FatPetStatusCode(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }
}
