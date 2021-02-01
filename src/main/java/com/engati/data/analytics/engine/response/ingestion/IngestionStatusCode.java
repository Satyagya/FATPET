package com.engati.data.analytics.engine.response.ingestion;

public enum IngestionStatusCode {
  SUCCESS(1000, "SUCCESS"),
  FAIL(15001, "FAIL");

  private final int code;

  private final String desc;

  private IngestionStatusCode(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  /**
   * Return the integer code of this status code.
   */
  public int getCode() {
    return this.code;
  }

  /**
   * Return the reason phrase of this status code.
   */
  public String getDesc() {
    return desc;
  }

  /**
   * Return a string representation of this status code.
   */
  @Override
  public String toString() {
    return Integer.toString(code);
  }
}

